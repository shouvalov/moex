import requests
import json


def print_prev_admitted_quote():
    url = 'http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?securities=SBER,GAZP,YNDX&iss.only=securities&iss.dp=comma&iss.meta=off&securities.columns=PREVDATE,SECID,PREVADMITTEDQUOTE'

    request = requests.get(url)
    jres = json.loads(request.content.decode('utf-8'))

    jcols = jres['securities']['columns']
    jdata = jres['securities']['data']

    prevDateIdx = jcols.index('PREVDATE')
    secIdx = jcols.index('SECID')
    priceIdx = jcols.index('PREVADMITTEDQUOTE')

    print('\r\nдата закрытия / Id / цена закрытия')
    for sec in jdata:
        print(sec[prevDateIdx], sec[secIdx], sec[priceIdx])

def print_marketdata():
    url = 'http://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?securities=SBER,GAZP,YNDX&iss.only=marketdata&iss.dp=comma&iss.meta=off&marketdata.columns=UPDATETIME,SECID,LAST'

    request = requests.get(url)
    jres = json.loads(request.content.decode('utf-8'))

    jcols = jres['marketdata']['columns']
    jdata = jres['marketdata']['data']

    updateTimeIdx = jcols.index('UPDATETIME')
    secIdx = jcols.index('SECID')
    priceIdx = jcols.index('LAST')

    print('\r\nвремя обновления / Id / текущая цена')
    for sec in jdata:
        print('%s %s %s' % (sec[updateTimeIdx], sec[secIdx], str(sec[priceIdx])))


def main():
    print_prev_admitted_quote()
    print('')
    print_marketdata()


if __name__ == '__main__':
    main()
