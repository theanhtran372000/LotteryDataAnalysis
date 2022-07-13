import streamlit as st
import pandas as pd
import time
from datetime import datetime
import yaml

st.set_page_config(layout='wide')
st.title('Phân tích dữ liệu lô đề')

# Kết quả 100 ngày gần đây
df = pd.read_csv('./results/100.csv')
result = df.values

# --- Đề 2 số --- #
st.subheader('Đề 2 số')
st.markdown('**Kết quả những ngày gần đây**')
latest_n2 = st.select_slider('Số ngày', range(1, 11), value=3, key='latest_n2')
latest_col2 = st.columns(latest_n2)
df2_latest_list = []
for i in range(latest_n2):
    df2_latest_list.append(latest_col2[i].metric('Ngày {}'.format(datetime.strptime(result[i][0], '%Y-%m-%d').strftime('%d/%m/%Y')), 'Số {:02}'.format(int(str(result[i][1])[-2:]))))

st.markdown('**Thống kê số lần xuất hiện của mỗi số đề**')
df2 = pd.read_csv('./results/2digit.csv')
df2_graph = st.bar_chart(df2.set_index('number'))

st.markdown('**Những số đề trúng nhiều nhất**')
df2_sorted = sorted([(i[1], str(i[0])) for i in df2.values], reverse=True)

top_n2 = st.select_slider('Top đề 2 số', range(1, 11), value=3)
col2 = st.columns(top_n2)
df2_top_list = []
for i in range(top_n2):
    df2_top_list.append(col2[i].metric('Top {}'.format(i + 1), 'Số {:02d}'.format(int(df2_sorted[i][1])), '{} lần'.format(df2_sorted[i][0])))

# --- Đề 3 số --- #
st.subheader('Đề 3 số')
st.markdown('**Kết quả những ngày gần đây**')
latest_n3 = st.select_slider('Số ngày', range(1, 11), value=3, key='latest_n3')
latest_col3 = st.columns(latest_n3)
df3_latest_list = []
for i in range(latest_n3):
    df3_latest_list.append(latest_col3[i].metric('Ngày {}'.format(datetime.strptime(result[i][0], '%Y-%m-%d').strftime('%d/%m/%Y')), 'Số {:03}'.format(int(str(result[i][1])[-3:]))))

st.markdown('**Thống kê số lần xuất hiện của mỗi số đề**')
df3 = pd.read_csv('./results/3digit.csv')
df3_graph = st.bar_chart(df3.set_index('number'))

st.markdown('**Những số đề trúng nhiều nhất**')
df3_sorted = sorted([(i[1], str(i[0])) for i in df3.values], reverse=True)

top_n3 = st.select_slider('Top đề 3 số', range(1, 11), value=3)
col3 = st.columns(top_n3)
df3_top_list = []
for i in range(top_n3):
    df3_top_list.append(col3[i].metric('Top {}'.format(i + 1), 'Số {:03d}'.format(int(df3_sorted[i][1])), '{} lần'.format(df3_sorted[i][0])))
    
# --- Đề chẵn lẻ --- #
st.subheader('Đề chẵn lẻ')
odd_cnt = 0     # Số lần lẻ
even_cnt = 0    # Số lần chẵn

for item in df2_sorted:
    number = int(item[1])
    if number % 2 == 0:
        even_cnt += int(item[0])
    else:
        odd_cnt += int(item[0])

total = odd_cnt + even_cnt
odd_rate = odd_cnt / total * 100
even_rate = 100 - odd_rate


st.markdown('**Số lần xuất hiện chẵn và lẻ**')
col = st.columns(3)
total_col = col[0].metric('Tổng số', '100%', '{} lần'.format(total))
odd_col = col[1].metric('Số lẻ', '{:.2f}%'.format(odd_rate), '{} lần'.format(odd_cnt))
even_col = col[2].metric('Số chẵn', '{:.2f}%'.format(even_rate),'{} lần'.format(even_cnt))

# while True:
#     print('- ' * 20)
#     print('Check data at {} ...'.format(datetime.now().strftime('%Y/%m/%d %H:%M:%S')))
#     with open('./config.yaml', 'r') as f:
#         config = yaml.load(f, yaml.loader.SafeLoader)
#         n_samples = config['hdfs_count']
        
#     # Có dữ liệu mới
#     if n_samples > total:
#         print('There are new data. Updating stats ...')
        
#         # Update dữ liệu đề 2 số
#         df2 = pd.read_csv('./results/2digit.csv')
        
#         # Update graph
#         df2_graph.bar_chart(df2.set_index('number'))

#         # Update top
#         df2_sorted = sorted([(i[1], str(i[0])) for i in df2.values], reverse=True)
#         for i in range(top_n2):
#             df2_top_list[i].metric('Top {}'.format(i + 1), 'Số {:02d}'.format(int(df2_sorted[i][1])), '{} lần'.format(df2_sorted[i][0]))
            
            
#         # Update dữ liệu đề 3 số
#         df3 = pd.read_csv('./results/3digit.csv')
        
#         # Update graph
#         df3_graph.bar_chart(df3.set_index('number'))

#         # Update top
#         df3_sorted = sorted([(i[1], str(i[0])) for i in df3.values], reverse=True)
#         for i in range(top_n3):
#             df3_top_list[i].metric('Top {}'.format(i + 1), 'Số {:03d}'.format(int(df3_sorted[i][1])), '{} lần'.format(df3_sorted[i][0]))
            
            
#         # Update đề chẵn lẻ
#         # Tính lại thông số
#         odd_cnt = 0     # Số lần lẻ
#         even_cnt = 0    # Số lần chẵn

#         for item in df2_sorted:
#             number = int(item[1])
#             if number % 2 == 0:
#                 even_cnt += int(item[0])
#             else:
#                 odd_cnt += int(item[0])

#         total = odd_cnt + even_cnt
#         odd_rate = odd_cnt / total * 100
#         even_rate = 100 - odd_rate
        
#         # Hiển thị lại
#         total_col.metric('Tổng số', '100%', '{} lần'.format(total))
#         odd_col.metric('Số lẻ', '{:.2f}%'.format(odd_rate), '{} lần'.format(odd_cnt))
#         even_col.metric('Số chẵn', '{:.2f}%'.format(even_rate),'{} lần'.format(even_cnt))
#         print('Finished!')
#     else:
#         print('Nothing changed!')
        
#     print()
#     time.sleep(5 * 60) # Check 5p 1 lần