import os
import json
import requests
import pandas as pd
from flask import Flask, request, Response


# constants
TOKEN = '1771775265:AAHWnc3hAVV6LlRNwfhufspPXsK6282SG0k'

# info about the bot
# 'https://api.telegram.org/bot{TOKEN}/getMe'


# get updates
# 'https://api.telegram.org/bot{TOKEN}/getUpdates'


# Webhook Heroku
# 'https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://al-rossmann-telegram-bot.herokuapp.com/'


def send_message(chat_id, text):
    method='sendMessage'
    url = 'https://api.telegram.org/bot{}/{}?chat_id={}'.format(TOKEN, method, chat_id)
    r = requests.post(url, json={'text': text})
    print('Status Code: {}'.format(r.status_code))

    return None


def load_dataset(store_id):
    # loading test dataset
    df10_test = pd.read_csv('./data/test.csv')
    df10_store = pd.read_csv('./data/store.csv')
    df10 = pd.merge(df10_test, df10_store, on='Store', how='left')

    # choose store for prediction
    df10 = df10[df10['Store'] == store_id]

    if not df10.empty:

        # removed closed
        df10 = df10[df10['Open'] != 0]
        df10 = df10[~df10['Open'].isnull()]
        df10 = df10.drop('Id', axis=1)

        # convert DataFrame to json
        data = json.dumps(df10.to_dict(orient='records'))
        
    else:
        data = 'error'

    return data


# API call
def predict(data):
    # url = 'http://127.0.0.1:5000/rossmann/predict'
    url = 'http://al-model-rossmann.herokuapp.com/rossmann/predict' # Heroku
    header = {'Content-type': 'application/json'}
    data = data

    r = requests.post(url, data=data, headers=header)
    print('Status code: {}'.format(r.status_code))

    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())

    return d1


def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    
    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)
    
    except ValueError:
        store_id = 'error'

    return chat_id, store_id


# API initialize
app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        message = request.get_json()
        chat_id, store_id = parse_message(message)

        if store_id != 'error':
            # loading data
            data = load_dataset(store_id)
            
            if data != 'error':
                # prediction
                d1 = predict(data)

                # calculation
                d2 = d1[['store', 'prediction']].groupby('store').sum().reset_index()

                # send message
                msg = 'Store number {} will sell R$ {:,.2f} in the next 6 weeks.'.format(
                    d2['store'].values[0],
                    d2['prediction'].values[0])

                send_message(chat_id, msg)
                return Response('Ok', status=200)

            else:
                send_message(chat_id, "Store not Available")
                return Response('Ok', status=200)


        else:
            send_message(chat_id, 'Store ID is Wrong')
            return Response('Ok', status=200)


    else:
        return '<h1> Rossmann Telegram BOT </h1>'


if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port)


