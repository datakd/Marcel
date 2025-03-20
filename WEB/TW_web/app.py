from flask import Flask, render_template, request, url_for, redirect, send_from_directory
from werkzeug.utils import secure_filename
import pandas as pd
import json
import requests
import threading
import time
from concurrent.futures import ThreadPoolExecutor
import os
import numpy as np
import warnings
from openpyxl import load_workbook

# Suppress the specific warning
warnings.simplefilter("ignore", UserWarning)

app = Flask(__name__)

UPLOAD_FOLDER = 'C:\\Users\\11021041\\Git\\Marcel\\WEB\\TW_web\\uploads'

ALLOWED_EXTENSIONS = {'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


# 檢查檔案擴展名是否符合允許的擴展名
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# 原始程式碼
def process_data(filename, action, username, table_id):
    try:
        # 取得 Token , 設置請求參數 (台灣)
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

        print(ac_token)

        def getUserID(username):
            # select from user 人員名單
            url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"

            headers = {
                "Authorization": f"Bearer {ac_token}",
                "Content-Type":"application/x-www-form-urlencoded"
            } 

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

            user_id = user_df.loc[user_df['name'] == username, 'id'].values.tolist()
            return user_id[0] if user_id else "找不到該使用者"
        
        user_id = getUserID(username)
        print(username)
        print(user_id)

        ### 取得user Token
        def fetch_data(userID):
            base_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/oauth/token/actions/getDelegateToken?delegateUserId="
            api_url = f"{base_url}{userID}"
            print(api_url)

            headers = {
                "Authorization": f"Bearer {ac_token}",
                "Content-Type":"application/x-www-form-urlencoded"
            } 
            
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
                print(f"Error accessing API for userID {userID} ; Status code: {response.status_code}")
                return None

        results = fetch_data(user_id)

        # 解析 JSON 字串
        data_dict = json.loads(results)

        # 取得 access_token
        ac_token = data_dict["access_token"]

        print(ac_token)


        #### 導入資料 ####
        # 使用者輸入檔案名稱，並加入錯誤處理
        try:
            Tasks_df1 = pd.read_excel(filename, dtype=str).rename(columns=str.lower)
            Tasks_df1['id'] = Tasks_df1['id'].str.replace(r'\D', '', regex=True).str.strip()

        except FileNotFoundError:
            print("錯誤：找不到指定的檔案。請確保檔案存在並提供正確的路徑。")
        except Exception as e:
            print(f"錯誤：{str(e)}")

        #### get procInstId ####
        Tasks_df2 = Tasks_df1[['id']]


        if action == 'withdraw':
            def fetch_data(data_id):
                headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {ac_token}"
                }
                print(data_id)
                base_url = f"https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/history/filter?entityApiKey={table_id}&dataId="
                api_url = f"{base_url}{data_id}&stageFlg=false"
                response = requests.get(api_url, headers=headers)
                if response.status_code == 200:
                    api_data = response.json()
                    if api_data["data"]:
                        last_data_index = len(api_data["data"]) - 1  # 找到最後一筆資料的索引
                        if "procInstId" in api_data["data"][last_data_index]:
                            print(api_data["data"][last_data_index]["procInstId"])
                            return json.dumps({"dataId": data_id, "procInstId": api_data["data"][last_data_index]["procInstId"]})
                        else:
                            print("Last data does not contain 'procInstId'")
                            return
                    else:
                        print("Data is empty")
                        return
                else:
                    print(f"Error accessing API for dataId {data_id}. Status code: {response.status_code}")
                    return
                
            # Set the maximum number of threads you want to use
            max_threads = 2  # You can adjust this based on your needs

            with ThreadPoolExecutor(max_threads) as executor:
                # Use executor.map to asynchronously fetch data for each row in parallel
                dfs = list(executor.map(fetch_data, Tasks_df2['id']))
                # Add a time delay between each thread

            json_data = [entry for entry in dfs if entry is not None]
            Tasks_df2 = pd.json_normalize([json.loads(entry) for entry in json_data])

            Tasks_df2 = Tasks_df2.astype(str)


            #### withdraw_task ####
            def withdraw_task(row, table_id):
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
                        "procInstId": task_id,
                    }
                }

                response = requests.post(url_2, headers=headers, json=data)
                result = response.json()
                print(f"Response for dataId {data_id}: {result}")


            def process_rows_with_threads(rows, table_id):
                threads = []
                for index, row in rows.iterrows():
                    thread = threading.Thread(target=withdraw_task, args=(row, table_id))
                    threads.append(thread)
                    thread.start()
                    time.sleep(0.08)

                # Wait for all threads to finish
                for thread in threads:
                    thread.join()


            data_ids_df = Tasks_df2[['dataId', 'procInstId']]

            # 使用多執行緒處理資料
            process_rows_with_threads(data_ids_df, table_id)


        elif action == 'submit':
            #### get preProcessor 獲取下一步信息 ####
            def preProcessor(row, table_id):
                data_id = row['id']
                url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task/actions/preProcessor"
                headers = {
                    "Authorization": f"Bearer {ac_token}",
                    "Content-Type": "application/json"
                }

                data = {
                    "data": {
                        "action": "submit",
                        "entityApiKey": table_id,
                        "dataId": data_id
                    }
                }

                response = requests.post(url_2, headers=headers, json=data)

                # Check if the request was successful
                if response.status_code == 200:
                    result = response.json()

                    print(f"Response for dataId {data_id}: {result}")

                    # Return relevant information from the result
                    return {
                        "data_id": data_id,
                        "result": result
                    }
                else:
                    print(f"Error accessing API for dataId {data_id}. Status code: {response.status_code}")
                    return None  # 或者返回一個合適的錯誤信息

            # Apply the function to each row and collect results
            results = Tasks_df1.apply(preProcessor, axis=1, table_id=table_id)

            results_df = pd.json_normalize(results)
            results_df = results_df[results_df['result.data.chooseApprover'].notnull()]
            results_df1 = results_df[['result.data.chooseApprover']]

            choose_approver_df = pd.json_normalize(results_df1['result.data.chooseApprover'].explode())
            choose_approver_df = choose_approver_df.astype(str)
            choose_approver_df['id'] = choose_approver_df['id'].astype(str).str.split('.').str[0].astype(np.int64)

            results_df = results_df.reset_index(drop=True)
            choose_approver = choose_approver_df.reset_index(drop=True)

            results_df2 = pd.concat([results_df, choose_approver], axis=1)

            results_df2['result.data.procdefId'] = results_df2['result.data.procdefId'].astype(str).str.split('.').str[0].astype(np.int64)

            #### submit_task ####
            def submit_task(row, table_id, userID):
                data_id = row['data_id']
                task_id = row['id']
                procdefId_id = row['result.data.procdefId']
                
                # Extracting nextTaskDefKey from the list
                next_user_tasks = row['result.data.nextUserTasks']
                nextTaskDefKey = next_user_tasks[0]['nextTaskDefKey'] if next_user_tasks else None

                url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
                headers = {
                    "Authorization": f"Bearer {ac_token}",
                    "Content-Type": "application/json"
                }

                # Updated nextAssignees/ccs list
                data = {
                    "data": {
                        "action": "submit",
                        "entityApiKey": table_id,
                        "dataId": data_id,
                        "procdefId": procdefId_id,
                        "nextTaskDefKey": nextTaskDefKey,
                        "nextAssignees": [task_id],
                        "ccs": [task_id],
                        "submitter": userID  
                    }
                }

                response = requests.post(url_2, headers=headers, json=data)
                result = response.json()
                print(f"Response for dataId {data_id}: {result}")

            # Assuming data_ids is your DataFrame
            data_ids_df = results_df2[['data_id', 'id','result.data.procdefId', 'result.data.nextUserTasks']]

            # Create a thread for each row in the DataFrame
            threads = []
            for index, row in data_ids_df.iterrows():
                thread = threading.Thread(target=submit_task, args=(row, table_id, user_id))
                threads.append(thread)
                thread.start()
                time.sleep(0.08)

            # Wait for all threads to finish
            for thread in threads:
                thread.join()
        
        result = "success"  # 假設成功
        success_message = "執行成功！"  # 新增成功訊息

    except Exception as e:
        error_message = f"錯誤：{str(e)}"
        result = error_message  # 如果出現錯誤，將錯誤消息賦值給 result
        success_message = None  # 若有錯誤，將成功訊息設為 None

    return result, success_message


def upload_file():
    if request.method == 'POST':
        try:
            # 檢查是否有上傳檔案
            if 'file' not in request.files:
                return "錯誤：沒有選擇檔案。"
            
            file = request.files['file']

            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
                return redirect(url_for('uploaded_file', filename=filename))
            
            return '''
            執行成功
            '''
        except Exception as e:
            error_message = f"錯誤：{str(e)}"
            return render_template('index.html', error_message=error_message)

    return render_template('index.html')


# 定義首頁路由
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # 檢查是否有上傳檔案
            if 'file' not in request.files:
                return "錯誤：沒有選擇檔案。"
            
            file = request.files['file']

            
            # 檢查檔案名稱和擴展名是否合法
            if file.filename == '':
                return "錯誤：檔案名稱不能為空。"
            if not allowed_file(file.filename):
                return "錯誤：不允許的檔案擴展名。僅接受.xlsx檔案。"

            # 儲存上傳的檔案
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(file_path)
            
            # 獲取用戶的操作
            action = request.form.get('action')

            # 取得提交人名稱
            username = request.form.get('username')

            # 取得對象
            table_id = request.form.get('table_id')

            # 使用多執行緒處理資料
            result, success_message = process_data(file_path, action, username, table_id)

            os.remove(file_path)  # 刪除上傳的檔案

        except Exception as e:
            error_message = f"錯誤：{str(e)}"
            result, success_message = error_message, None
        
        return render_template('index.html', result=result, success_message=success_message)

    return render_template('index.html')

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(host='192.168.2.27', port=5000, debug=True)

