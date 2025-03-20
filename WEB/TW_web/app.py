import os
import pandas as pd
import requests
import json
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from concurrent.futures import ThreadPoolExecutor
import threading
import time

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"  # 確保有這個目錄
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return render_template('index.html', result="未選擇檔案")
    
    file = request.files['file']
    if file.filename == '':
        return render_template('index.html', result="未選擇檔案")
    
    if not file.filename.endswith('.xlsx'):
        return render_template('index.html', result="請上傳.xlsx格式的檔案")
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(file_path)
    
    try:
        df = pd.read_excel(file_path)
        if 'id' not in df.columns:
            return render_template('index.html', result="檔案缺少 'id' 欄位")
    except Exception as e:
        return render_template('index.html', result=f"讀取檔案時發生錯誤: {e}")
    
    table_id = request.form.get("table_id")
    username = request.form.get("username")
    action = request.form.get("action")

    ### 取得user token

    try:
        # 先取得自己的 Token , 設置請求參數 (台灣)
        url = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "password",
            "client_id": "bc4896f9e3e4fad682f9fe60d5fbaa2e",
            "client_secret": "3407faf39a38b6821f18cdbe019a56e3",
            "redirect_uri": "https://api-p10.xiaoshouyi.com",
            "username": "11021041@twkd.com",
            "password": "Marcellin01PrRDrSdz"
        }

        # 發送 post 請求
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            print(response.json())
        else:
            print("請求失敗, 狀態碼:", response.status_code)

        content = response.json()
        ac_token = content["access_token"]

        headers = {
            "Authorization": f"Bearer {ac_token}",
            "Content-Type":"application/x-www-form-urlencoded"
        } 
        
        # select from user 人員名單
        url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"

        queryLocator = ''
        user_df = pd.DataFrame()

        while True:
            data = {
                "xoql": f'''select 
                id, 
                name 
                FROM user
                ''',
                "batchCount": 2000,
                "queryLocator": queryLocator
            }
            response = requests.post(url_2, headers=headers, data=data)
            crm = response.json()
            data = pd.DataFrame(crm["data"]["records"])
            user_df = pd.concat([user_df, data], ignore_index=True, sort=False)

            if not crm['queryLocator']:
                break
            queryLocator = crm['queryLocator'] 

        if user_df.empty:
            return render_template('index.html', result=f"找不到使用者 {username}")
        user_df = user_df[user_df['name'] == username]
        user_id = user_df['id'].iloc[0]

        print(username)
        print(user_id)


        # 銷售易URL(代理userid)
        base_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/oauth/token/actions/getDelegateToken?delegateUserId="

        def fetch_data(userID):
            api_url = f"{base_url}{userID}"
            print(api_url)
            
            response = requests.get(api_url, headers=headers)
            if response.status_code == 200:
                api_data = response.json()
                print(api_data)
                
                if api_data.get("result"):
                    return json.dumps({"userID": userID, "access_token": api_data["result"]['access_token']})
                else:
                    print("Data is empty")
                    return None
            else:
                print(f"Error accessing API for userID {userID}. Status code: {response.status_code}")
                return None

        results = fetch_data(user_id)

        # 解析 JSON 字串
        data_dict = json.loads(results)

        # 取得 access_token
        ac_token = data_dict["access_token"]

        print(ac_token)

        # 執行動作
        if action == "submit":

            ### submit_task ###
            # API URL
            status_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task/actions/preProcessor"
            submit_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"

            # 取得 approvalStatus
            def preProcessor(process_ID):
                status_body = {
                    "data": {
                        "action": "submit",
                        "entityApiKey": table_id,
                        "dataId": process_ID
                    }
                }
                try:
                    response = requests.post(status_url, headers=headers, json=status_body)
                    response.raise_for_status()
                    crm_json = response.json().get('data', {})
                    return crm_json
                except requests.exceptions.RequestException as e:
                    print(f"Error in preProcessor for ID {process_ID}: {e}")
                    return None

            # 提交 task
            def submit_task(row):
                data_id = row.id  # 使用 itertuples，所以用 row.id
                approval_status = preProcessor(data_id)
                
                if not approval_status:
                    print(f"Skipping dataId {data_id} due to error in preProcessor")
                    return
                
                try:
                    approvers = approval_status.get('chooseApprover', [])
                    if not approvers:
                        print(f"No approver found for dataId {data_id}, skipping...")
                        return
                    
                    approver_id = approvers[0]['id']  # 確保是對應的 approver
                    
                    data = {
                        "data": {
                            "action": "submit",
                            "entityApiKey": table_id,
                            "dataId": data_id,
                            "procdefId": approval_status.get('procdefId'),
                            "nextTaskDefKey": approval_status.get('nextTaskDefKey'),
                            "nextAssignees": [approver_id],  
                            "ccs": [approver_id]
                        }
                    }

                    response = requests.post(submit_url, headers=headers, json=data)
                    response.raise_for_status()
                    result = response.json()
                    print(f"Response for dataId {data_id}: {result}")

                except requests.exceptions.RequestException as e:
                    print(f"Error submitting task for dataId {data_id}: {e}")

            # 多線程執行，確保每筆資料的 approvalStatus 是對應的
            with ThreadPoolExecutor(max_workers=10) as executor:  # 10 個線程同時執行
                executor.map(submit_task, df.itertuples(index=False))
            

            message = f"成功提交 {len(df)} 筆資料（表格: {table_id}, 使用者: {username}）"

        if action == "withdraw":
            ### get procInstId ###
            Tasks_df2 = df[['id']]
            base_url = f"https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/history/filter?entityApiKey={table_id}&dataId="

            def fetch_data(data_id):
                print(data_id)
                api_url = f"{base_url}{data_id}&stageFlg=false"
                response = requests.get(api_url, headers=headers)
                if response.status_code == 200:
                    api_data = response.json()
                    if api_data["data"]:
                        print(api_data["data"][-1]["procInstId"])
                        return json.dumps({ "dataId": data_id, "procInstId": api_data["data"][-1]["procInstId"] })
                    else:
                        print("Data is empty")
                        return
                else:
                    print(f"Error accessing API for dataId {data_id}. Status code: {response.status_code}")
                    return           

            max_threads = 2 

            with ThreadPoolExecutor(max_threads) as executor:
                dfs = list(executor.map(fetch_data, Tasks_df2['id']))

            json_data = [entry for entry in dfs if entry is not None]
            Tasks_df2 = pd.json_normalize([json.loads(entry) for entry in json_data])

            ### withdraw_task ###
            def withdraw_task(row):
                data_id = row['dataId']
                task_id = row['procInstId']
                url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
                headers = {
                    "Authorization": f"Bearer {ac_token}",
                    "Content-Type": "application/json"
                }
                data = {
                    "data": {
                        "action": "withdraw",
                        "entityApiKey": table_id,
                        "dataId": data_id,
                        "procInstId": task_id
                    }
                }

                response = requests.post(url_2, headers=headers, json=data)
                result = response.json()
                print(f"Response for dataId {data_id}: {result}")

            # Assuming data_ids is your DataFrame
            data_ids_df = Tasks_df2[['dataId', 'procInstId']]

            # Create a thread for each row in the DataFrame
            threads = []
            for index, row in data_ids_df.iterrows():
                thread = threading.Thread(target=withdraw_task, args=(row,))
                threads.append(thread)
                thread.start()
                time.sleep(0.08)

            # Wait for all threads to finish
            for thread in threads:
                thread.join()

            message = f"成功撤回 {len(df)} 筆資料（表格: {table_id}, 使用者: {username}）"

        flash(message, "success")

    except Exception as e:
        flash(f"處理文件時發生錯誤: {str(e)}", "error")
        return render_template('index.html', result=str(e))

    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"已刪除上傳文件: {file_path}")
    
    return render_template('index.html', success_message=message)

if __name__ == "__main__":
    app.run(debug=True)
