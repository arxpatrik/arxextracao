import datetime

ontem = datetime.datetime.now() - datetime.timedelta(days=1)
print(ontem.strftime('%Y-%m-%d'))
