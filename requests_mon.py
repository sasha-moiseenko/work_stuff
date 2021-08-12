import mysql.connector


cnx = mysql.connector.connect(user='bash', password='456redko',
                              host='10.12.88.11',
                              database='rip')
cursor = cnx.cursor()
query=("SELECT p.name as provider, p.rapi_endpoint as endpoint,  t.short_description as style,  p.rapi_request_status_id as status, COUNT(*) as count FROM rapi_requests r JOIN providers p ON p.id = r.provider_id  JOIN rapi_request_types t ON t.id = r.rapi_request_type_id JOIN rapi_request_statuses s ON s.id = r.rapi_request_status_id WHERE rapi_request_status_id < 95 GROUP BY provider, endpoint, style, status;")

cursor.execute(query)
for (provider, endpoint, style, status, count) in cursor:
    print(f'requests_stat,provider={provider} endpoint={endpoint},type={style},status={status},value={count}')

cursor.close()
cnx.close()

