"""
Conection to Cloudant and Load to a DB2 database to feed a cognos DashBoard
https://eu-gb.dataplatform.cloud.ibm.com/dashboards/f090b7a0-3b4d-4c29-a072-6983b07e65b5/view/5b1fd40238b232cc50c3d4e407cd7d042e622d55bbbbd00484847b490a652397a8381097c87e4f5c88195366f3bf1a5fcc
"""

from cloudant.client import Cloudant
import ibm_db
import pandas as pd
credentials = {
  "apikey": "blhBDPGj2s8bL7NlKXGJwq1csCPPaDTlAxHyTYKFlZXY",
  "host": "9bd1f125-3a80-48a1-921e-bbffaa9ca651-bluemix.cloudantnosqldb.appdomain.cloud",
  "iam_apikey_description": "Auto generated apikey during resource-key operation for Instance - crn:v1:bluemix:public:cloudantnosqldb:eu-gb:a/a475d56606a64adf862c29277ce25ea9:411b0d06-2cc5-41ba-86d1-08dedb4ff5e6::",
  "iam_apikey_name": "auto-generated-apikey-a43adaec-b315-49b5-957b-035831e33d47",
  "iam_role_crn": "crn:v1:bluemix:public:iam::::serviceRole:Manager",
  "iam_serviceid_crn": "crn:v1:bluemix:public:iam-identity::a/a475d56606a64adf862c29277ce25ea9::serviceid:ServiceId-fe5219c7-6b98-4b2e-b859-0a4bd7139206",
  "password": "c12f1bec253139b62ff291250a2058b0624c1d0b7578dc851b7793086fd9f9fe",
  "port": 443,
  "url": "https://9bd1f125-3a80-48a1-921e-bbffaa9ca651-bluemix:c12f1bec253139b62ff291250a2058b0624c1d0b7578dc851b7793086fd9f9fe@9bd1f125-3a80-48a1-921e-bbffaa9ca651-bluemix.cloudantnosqldb.appdomain.cloud",
  "username": "9bd1f125-3a80-48a1-921e-bbffaa9ca651-bluemix"
}

client = Cloudant(credentials['username'],credentials['password'],url=credentials['url'])
client.connect()
session = client.session()
print('Username: {0}'.format(session['userCtx']['name']))
print('Databases: {0}'.format(client.all_dbs()))
client.session_login(credentials['username'],credentials['password'])
my_database = client['madrid201907252141'].all_docs(include_docs=True)['rows']

print("conecting...")
N = len(my_database)
# general information
# Function that returns general information.
def query_pers_inf(name_object, name_field, name_gen = 'doc', name_id = 'id_ord'):
  return pd.DataFrame([[str(doc[name_gen][name_id]),doc[name_gen][name_object][name_field]] \
                       for doc in my_database]).rename(index=str,columns={0:'id',1:name_field}).set_index('id')

# People's age
age = query_pers_inf('personal_information','age')
# People's profession
profession = query_pers_inf('personal_information','profession')
# People's status in emergency
status_emergency = query_pers_inf('personal_information','status_emergency')
# People's language
languages = query_pers_inf('personal_information','languages')
# People's last loacation
last_location = query_pers_inf('personal_information','last_location')
# People's marital status
marital_status = query_pers_inf('personal_information','marital')
# People's gender
gender = query_pers_inf('personal_information','gender')
# People's general health
health = query_pers_inf('personal_information','health')
# People's timestamp
timestamp = pd.DataFrame([[str(doc['doc']['id_ord']), doc['doc']['timestamp']] \
                       for doc in my_database]).rename(index=str,columns={0:'id',1:'timestamp'}).set_index('id')
last_location['camp_A'] = 0
last_location['camp_B'] = 0
last_location['lat'] = None
last_location['lon'] = None
last_location.loc[last_location['last_location'] == 'camp_A','camp_A'] = 1
last_location.loc[last_location['last_location'] == 'camp_B','camp_B'] = 1
last_location.loc[last_location['camp_A']+last_location['camp_B'] == 0 ,'lat'] = \
  last_location.loc[last_location['camp_A']+last_location['camp_B'] == 0 ,'last_location'].apply(lambda x: x[0])
last_location.loc[last_location['camp_A']+last_location['camp_B'] == 0 ,'lon'] = \
  last_location.loc[last_location['camp_A']+last_location['camp_B'] == 0 ,'last_location'].apply(lambda x: x[1])
del last_location['last_location']
gen_inf = pd.concat([age,profession,status_emergency,languages,last_location,marital_status,gender,health,timestamp],
                    axis=1).sort_values(by='timestamp')
gen_inf['day'] = gen_inf['timestamp'].str[3:5].astype(int)
gen_inf['month'] = gen_inf['timestamp'].str[:2].astype(int)
gen_inf['year'] = gen_inf['timestamp'].str[6:10].astype(int)
gen_inf['hour'] = gen_inf['timestamp'].str[10:12].astype(int)
gen_inf['min'] = gen_inf['timestamp'].str[13:15].astype(int)
gen_inf['sec'] = gen_inf['timestamp'].str[17:].astype(int)

# facilities log
# Function that returns facilities log information.
def query_log(name_object, name_field, prefix, name_gen = 'doc', name_id = 'id_ord',name_event='event'):
  return pd.DataFrame([[doc[name_gen][name_id],doc[name_gen][name_object][name_event][name_field]] \
                       for doc in my_database]).rename(index=str,columns={0:'id',1:prefix+name_field}).set_index('id')
# Impact on facilities
fac_impact = query_log('log_facilities','impact',prefix='fac_')
fac_lat = query_log('log_facilities','lat',prefix='fac_')
fac_lon = query_log('log_facilities','lon',prefix='fac_')
fac_timestamp = query_log('log_facilities','timestamp',prefix='fac_')

# Impact on reported people
peop_need = query_log('log_people','need',prefix='peop_')
peop_lat = query_log('log_people','lat',prefix='peop_')
peop_lon = query_log('log_people','lon',prefix='peop_')
peop_timestamp = query_log('log_people','timestamp',prefix='peop_')

log = pd.concat([fac_impact,fac_lat,fac_lon,fac_timestamp,peop_need,peop_lat,peop_lon,peop_timestamp],axis=1)
prefix = 'peop_'
log.loc[log[prefix+'timestamp'].notna(),prefix+'day'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[3:5].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'month'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[:2].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'year'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[6:10].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'hour'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[10:12].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'min'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[13:15].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'sec'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[17:].astype(int)

prefix = 'fac_'
log.loc[log[prefix+'timestamp'].notna(),prefix+'day'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[3:5].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'month'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[:2].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'year'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[6:10].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'hour'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[10:12].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'min'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[13:15].astype(int)
log.loc[log[prefix+'timestamp'].notna(),prefix+'sec'] = \
  log.loc[log[prefix+'timestamp'].notna(),prefix+'timestamp'].str[17:].astype(int)


credencial_db2 = {
  "hostname": "dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net",
  "password": "ls@d9lsx089q5mg7",
  "https_url": "https://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:8443",
  "port": 50000,
  "ssldsn": "DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;PORT=50001;PROTOCOL=TCPIP;UID=rbs55704;PWD=ls@d9lsx089q5mg7;Security=SSL;",
  "host": "dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net",
  "jdbcurl": "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB",
  "uri": "db2://rbs55704:ls%40d9lsx089q5mg7@dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB",
  "db": "BLUDB",
  "dsn": "DATABASE=BLUDB;HOSTNAME=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=rbs55704;PWD=ls@d9lsx089q5mg7;",
  "username": "rbs55704",
  "ssljdbcurl": "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50001/BLUDB:sslConnection=true;"
}

db2 = ibm_db.pconnect(credencial_db2['dsn'],"","")


q1 = "CREATE TABLE stats ("\
    "id INTEGER,"\
    "timestamp DOUBLE,"\
    "n_people INTEGER,"\
    "n_centres INTEGER,"\
    "n_chatbt INTEGER,"\
    "n_fac_reported INTEGER,"\
    "n_peop_reported INTEGER,"\
    "n_injured INTEGER,"\
    "n_ser_injured INTEGER,"\
    "n_chronical INTEGER,"\
    "n_professional INTEGER,"\
    "mean_age DOUBLE,"\
    "period_rate DOUBLE,"\
    "n_under_age INTEGER,"\
    "n_males INTEGER,"\
    "n_females INTEGER,"\
    "n_serious_impacted INTEGER"\
    ");"

# Dropping table
drop = False
if drop:
  q_drop = "DROP TABLE gen_info;"

# Deleting table
delete = True
if delete:
  q_delete = "DELETE * FROM gen_info"
ibm_db.exec_immediate(db2,q_drop)
# Creating table
create = True

q_tmp = ' CREATE TABLE gen_info ('
names_gen_info = []
l_elements = []
for  col, tipo in pd.DataFrame(gen_inf.dtypes).iterrows():

  if tipo[0] == object or tipo[0] == str:
    t_fil = 'VARCHAR(30)'
  else:
    t_fil = 'DOUBLE'
  l_elements.append(" "+col+" "+ t_fil)
  names_gen_info.append(col)
tmp_fields = ", ".join(l_elements)
q_gen_inf = q_tmp + tmp_fields + ");"
if create:
  ibm_db.exec_immediate(db2,q_gen_inf)

q_tmp = ' INSERT INTO gen_info ( '+ ", ".join(names_gen_info) +' ) values ('
for linea in gen_inf.iterrows():
  l_tmp = []
  for element in linea[1].values.tolist():
    if type(element) is int or type(element) is float:
      l_tmp.append(str(element))
    elif element is not None:
      l_tmp.append("'"+element+"'")
    else:
      l_tmp.append("'none'")
  q = ", ".join(l_tmp) + ");"
  ibm_db.exec_immediate(db2, q_tmp + q)


