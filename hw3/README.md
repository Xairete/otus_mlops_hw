# otus_mlops_hw3

Точка доступа к бакету для дз 2: s3://elenagerman-mlops-hw2
Точка доступа к новому бакету с очищенными данными: s3://mlops-german-hw3/

В процессе работы над домашним заданием были выполнены следующие шаги:
- создан бакет для хранения обработанных данных
- создан data proc с необходимыми настройками, к нему получен доступ по ssh
- на мастер-ноде запущен jupyter notebook, в котором проведено EDA (установлена спарк сессия, данные прочитаны и обработаны с помощью спарк датафрейма)
- подготовлен скрипт очистки данных (сохранен в данном репозитории, скрипт hw3_v1.py)
- скрипт запущен на мастер-ноде, очищенные данные сохранены в новый бакет

Были выявлены следующие проблемы:
- дублирование записей по transaction_id, решение - удалить дубликаты записей
- при переводе tx_datetime в формат TimestampType() некоторые строки переводятся с ошибкой, что приводит к значению Null, решение - удалить эти записи
- при переводе некоторых записей в формат IntegerType в terminal_id некоторые строки переводятся с ошибкой, что приводит к значению Null, решение - удалить эти записи

Также выявлены следующие особенности, пока что данные оставлены без изменения:
- значение -999999 в поле customer_id. Возможно, это так были заполнены пропуски айди? пока не трогала, посмотрю дальше, как это учесть
- распределение количества транзакций по terminal_id. Количество транзакций по трем terminal_id на порядок превышает количество транзакций по другим айди. Возможно, стоит учесть это в фичах. 
