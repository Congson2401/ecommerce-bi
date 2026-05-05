content = """ecommerce_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: "24012004"
      port: 5432
      dbname: ecommerce_db
      schema: dw
      threads: 4
"""
with open(r'C:\Users\sonle\.dbt\profiles.yml', 'w') as f:
    f.write(content)
print('Done!')