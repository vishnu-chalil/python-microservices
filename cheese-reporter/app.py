from flask import Flask
from threading import Thread
import report_service

app = Flask(__name__)


@app.route('/report', methods=['GET'])
def generate_report():
    return report_service.generate_report()

with app.app_context():
    t = Thread(target=report_service.start_consumer)
    t.start()    

if __name__ == '__main__':
    app.run(port=5001)



