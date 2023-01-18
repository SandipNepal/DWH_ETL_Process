from credentials import secrets

import snowflake.connector

ctx = snowflake.connector.connect(
    user=secrets.get('user'),
    password=secrets.get('password'),
    account=secrets.get('account'),
    warehouse='COMPUTE_WH',
    database='BHATBHATENI'
)
cs = ctx.cursor()
