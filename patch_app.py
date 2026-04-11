import re

with open('app_flask.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
skip = False

for i, line in enumerate(lines):
    if skip:
        if "leads_to_process = leads_df.to_dict('records')" in line or "leads_to_export = leads_df.to_dict('records')" in line:
            skip = False
        continue
        
    # Chunk 1
    if "query = \"\"\"SELECT id, business_name, state, address, domain, website" in line and "ORDER BY CASE" in lines[i+3]:
        repl = '''            leads_df = db.get_all_leads()
            if not leads_df.empty:
                if 'owner_name' in leads_df:
                    leads_df = leads_df[leads_df['owner_name'].isna() | leads_df['owner_name'].eq('')]
                if 'domain' in leads_df.columns:
                    leads_df['has_domain'] = leads_df['domain'].notna() & (leads_df['domain'] != '')
                else:
                    leads_df['has_domain'] = False
                if 'created_at' in leads_df.columns:
                    leads_df = leads_df.sort_values(by=['has_domain', 'created_at'], ascending=[False, False])
                leads_to_process = leads_df.head(limit).to_dict('records')\n'''
        new_lines.append(repl)
        skip = True
        continue
        
    # Chunk 2
    if 'query = """SELECT id, business_name, state, owner_name, website, domain' in line and "WHERE owner_name IS NOT NULL" in lines[i+2]:
        repl = '''    leads_df = db.get_all_leads()
    if not leads_df.empty:
        if 'owner_name' in leads_df.columns and 'linkedin' in leads_df.columns:
            leads_df = leads_df[leads_df['owner_name'].notna() & (leads_df['owner_name'] != '') & leads_df['linkedin'].isna()]
        if 'created_at' in leads_df.columns:
            leads_df = leads_df.sort_values('created_at', ascending=False)
        leads_to_process = leads_df.head(limit).to_dict('records')\n'''
        new_lines.append(repl)
        skip = True
        continue
        
    # Chunk 3
    if 'query = """SELECT id, business_name, state, address, city' in line and 'WHERE (website IS NULL OR' in lines[i+2]:
        repl = '''   leads_df = db.get_all_leads()
   if not leads_df.empty:
       website_empty = leads_df.get('website', pd.Series(dtype=str)).fillna('').astype(str).eq('')
       domain_empty = leads_df.get('domain', pd.Series(dtype=str)).fillna('').astype(str).eq('')
       leads_df = leads_df[website_empty | domain_empty]
       if 'created_at' in leads_df.columns:
           leads_df = leads_df.sort_values('created_at', ascending=False)
       leads_to_process = leads_df.head(limit).to_dict('records')\n'''
        new_lines.append(repl)
        skip = True
        continue
        
    # Chunk 4
    if 'WHERE (ghl_exported IS NULL OR ghl_exported = 0)' in line and 'query = """SELECT * FROM leads' in lines[i-1]:
        # we actually hit the 'query =' line on the previous iteration. We need to replace backwards.
        # But wait, looking forward is better. I'll match on the 'query = ' line.
        pass

    if 'query = """SELECT * FROM leads' in line and 'WHERE (ghl_exported IS NULL OR ghl_exported = 0)' in lines[i+1]:
        repl = '''   leads_df = db.get_all_leads()
   if not leads_df.empty:
       if 'ghl_exported' in leads_df.columns:
           leads_df = leads_df[leads_df['ghl_exported'].isna() | (leads_df['ghl_exported'] == 0) | (leads_df['ghl_exported'] == '0') | (leads_df['ghl_exported'] == False)]
       if 'created_at' in leads_df.columns:
           leads_df = leads_df.sort_values('created_at', ascending=False)
       leads_to_export = leads_df.head(50).to_dict('records')\n'''
        new_lines.append(repl)
        skip = True
        continue

    new_lines.append(line)

with open('app_flask.py', 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
