import base64
import os
from datetime import datetime
import pandas as pd

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition

def send_email():
    now = pd.Timestamp.now()
    year = now.year
    month = now.month
    day = now.day
    # file_path = f'/Users/levanduy/PycharmProjects/fpt_ck/data/clean_data/{year}/{month}/{day}/hoatuoi_cleaned.csv'
    file_path = f'/opt/airflow/data/clean_data/{year}/{month}/{day}/hoatuoi_cleaned.csv'
    with open(file_path, 'rb') as f:
        data = f.read()
        f.close()

    encoded_file = base64.b64encode(data).decode()

    message = Mail(
        from_email='22655381.duy@student.iuh.edu.vn',
        to_emails='levanduy093@gmail.com',
        subject='Big Data - 22655381 - Nhom 6',
        html_content='Chao Co,<br>Em gui file data da duoc clean. Em xin cam on!')

    attached_file = Attachment(
        FileContent(encoded_file),
        FileName('hoa_tuoi.csv'),
        FileType('text/csv'),
        Disposition('attachment')
    )
    message.attachment = attached_file

    try:
        sg = SendGridAPIClient("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")
        response = sg.send(message)
        print(response.status_code)
        print(response.body)
        print(response.headers)
        print(datetime.now())
    except Exception as e:
        print(str(e))

# Use the function
send_email()