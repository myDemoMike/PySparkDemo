from flask import Flask, jsonify

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route('/')
def hello_world():
    return 'Hello World!'


from sklearn.externals import joblib


@app.route('/task/<year>/<month>/<day>/<hour>/<minute>/<second>', methods=['GET'])
def get_task(year, month, day, hour, minute, second):
    ss3 = joblib.load("E:/workspace/MachineLearning/003LinearRegression/result/data_ss.model")  # 加载模型
    lr3 = joblib.load("E:/workspace/MachineLearning/003LinearRegression/result/data_lr.model")  # 加载模型
    data1 = [[year, month, day, hour, minute, second]]
    try:
        data2 = ss3.transform(data1)
        result = lr3.predict(data2)[0]
    except:
        result = "请重新确认输入参数"
    return jsonify(result, "111",[12312312,12312])


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7418)
