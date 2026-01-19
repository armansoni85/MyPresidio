import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('/usr/share/data/icd10.db')
cursor = conn.cursor()

# Create indexes for fast searching
cursor.execute("CREATE INDEX IF NOT EXISTS idx_icd10_code ON icd10_data(code);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_icd10_short_desc ON icd10_data(short_desc);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_icd10_long_desc ON icd10_data(long_desc);")

# Commit changes and close connection
conn.commit()
conn.close()

print("✅ Indexes created successfully!")
import sqlite3

# Connect to your SQLite database
conn = sqlite3.connect('/usr/share/data/icd10.db')
cursor = conn.cursor()

# Create indexes for fast searching
cursor.execute("CREATE INDEX IF NOT EXISTS idx_icd10_code ON icd10_data(code);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_icd10_short_desc ON icd10_data(short_desc);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_icd10_long_desc ON icd10_data(long_desc);")

# Commit changes and close connection
conn.commit()
conn.close()

print("✅ Indexes created successfully!")

