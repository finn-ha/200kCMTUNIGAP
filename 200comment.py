import pandas as pd
from six import text_type
from sqlalchemy import create_engine
from sqlalchemy.types import Text

# Đọc file CSV
csv_path = 'G:/My Drive/Learn Python/csv_comments.csv'
df = pd.read_csv(csv_path,header=None,on_bad_lines='warn')

gomcot = df.values.ravel()
finaldata = pd.Series(gomcot).dropna()
finaldata = finaldata.drop_duplicates()

df = pd.DataFrame(finaldata,columns=['comments'])


# Define keywords
key = "vinh|tiên|vc|vợ chồng|vk ck|từ thiện|cứu trợ|đồng bào|tỷ|vc|quyên góp|miền trung|đồng bào|cv|tỷ|kiện|Hằng|lux"

# Filter across all columns
extract_df = df[df.apply(lambda row: row.astype(str).str.contains(key, case=False, na=False).any(), axis=1)]


# # Kết nối đến MySQL và đẩy data lên
username = 'root'
password = 'root'
db = 'long_db'
url = f"mysql+pymysql://{username}:{password}@localhost/{db}"
engine = create_engine(url)

extract_df.to_sql('extract_comment2', con=engine, if_exists='replace', index=False, dtype ={'comments': Text})





