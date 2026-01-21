from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/employees', methods=['GET'])
def get_employees():
    data = [
        {"id": 1, "name": "Jabir", "role": "Data Engineer"},
        {"id": 2, "name": "Sara", "role": "Analyst"}
    ]
    return jsonify(data)

if __name__ == '__main__':
    app.run(port=3000)