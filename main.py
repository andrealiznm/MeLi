# This program reads the body of gmail emails (which do not contain attachments) and stores the sender, subject and date in a mysql database.

from datetime import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from mysql.connector import connect
import base64

# Set up OAuth2 credentials
CLIENT_SECRETS_FILE = 'client_secret_MeLi.json'
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
creds = None
if not creds or not creds.valid:
    flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
    creds = flow.run_local_server(port=0)
service = build('gmail', 'v1', credentials=creds)

# Set up database connection
db = connect(
    host='104.154.113.196',
    user='meli',
    password='Crypt0.MeL12023',
    database='melidb'
)

# Search for emails with "DevOps" in the body
results = service.users().messages().list(userId='me', q='in:all DevOps').execute()
messages = results.get('messages', [])

# Iterate over emails and store them in the database
for message in messages:
    msg = service.users().messages().get(userId='me', id=message['id']).execute()
    parts = msg["payload"]["parts"]
    headers = msg['payload']['headers']
    date = None
    sender = None
    subject = None

    for header in headers:
        if header['name'] == 'Date':
            date = datetime.strptime(header['value'][:-6], '%a, %d %b %Y %H:%M:%S')
        elif header['name'] == 'From':
            sender = header['value']
        elif header['name'] == 'Subject':
            subject = header['value']

    for p in parts:
        if p["mimeType"] in ["text/plain", "text/html"]:
            data = base64.urlsafe_b64decode(p["body"]["data"]).decode("utf-8")
            #print(data)
        else:
            data = "archivoadjunto";

    if "archivoadjunto" in data:
        print("Este programa no lee correos con adjuntos")
    else:
        # Search word DevOps only in body
        if "DevOps" in data:
            if date and sender and subject:
                # Check if email already exists in the database
                cursor = db.cursor()
                cursor.execute("SELECT * FROM emails WHERE date=%s AND sender=%s AND subject=%s",
                               (date, sender, subject))
                result = cursor.fetchone()
                if not result:
                    # Insert new email into the database
                    #print(data)
                    cursor.execute("INSERT INTO emails (date, sender, subject) VALUES (%s, %s, %s)",
                                   (date, sender, subject))
                    db.commit()


db.close()
