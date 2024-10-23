import pandas as pd

def process_text(text):
    
    text = str(text)
    text = '' if text =='And' else text
    
    if '(Translated by Google)' in text:
        part = text.split('(Original)')[0]
        part = part.replace('(Translated by Google) ','')

        return part.replace('\r', '').replace('\t', '').replace('\n', '')
    else:
        return text.replace('\r', '').replace('\t', '').replace('\n', '')  # Return original text if marker is not found

def filter_dataframes(df):
        """Filters a DataFrame, dropping rows where the length of 'code' is not 36."""
        return df[df['code'].str.len() == 36]
    

def tag_questions_answers(df):
    df['question_tag'] = df.groupby('code').cumcount() + 1
    df['question_tag'] = 'Q' + df['question_tag'].astype(str)

    question_tags = df[['code', 'question', 'question_tag']].drop_duplicates().reset_index(drop=True)
    question_tags['question_tag'] = question_tags.groupby('code').cumcount() + 1
    question_tags['question_tag'] = 'Q' + question_tags['question_tag'].astype(str)
    
    question_mapping = question_tags.set_index(['code', 'question'])['question_tag'].to_dict()
    
    df['question_tag'] = df.set_index(['code', 'question']).index.map(question_mapping)
    
    df['answer_tag'] = df.groupby(['code', 'question_tag']).cumcount() + 1
    df['answer_tag'] = df.apply(lambda x: f"{x['question_tag']}.A{x['answer_tag']}", axis=1)
      
    return df


source = r"D:\DATA\2024\Oct\Output\FAQ\FAQ_(success).csv"
output_csv_path =r"D:\DATA\2024\Oct\Output\FAQ\17_10_FAQ.csv"

df = pd.read_csv(source,on_bad_lines='skip',encoding='utf-8-sig',encoding_errors='replace')

df.fillna('',inplace=True)
df=filter_dataframes(df)
df = df.applymap(process_text)
#df = df[df['content']!='']


# Apply the function
tagged_df = tag_questions_answers(df)

tagged_df=tagged_df[tagged_df['code'].apply(lambda x: len(x) == 36)]

# Assuming tagged_df is already defined
df1 = tagged_df[['code', 'question', 'question_tag']].copy()
df2 = tagged_df[['code', 'answer', 'answer_tag']].copy()

# Adding the 'type' column
df1['type'] = 'Question'
df2['type'] = 'Answer'

# Renaming columns
df1.rename(columns={'question': 'content', 'question_tag': 'section'}, inplace=True)
df2.rename(columns={'answer': 'content', 'answer_tag': 'section'}, inplace=True)

# Concatenating the DataFrames
concat_df = pd.concat([df1, df2], ignore_index=True)
concat_df['likes'] = ''
concat_df= concat_df[['code','type','section','likes','content']]

concat_df.to_csv(output_csv_path,index=False)