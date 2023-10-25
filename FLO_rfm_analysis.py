###############################################################
# RFM ile FLO Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

# 1. İş Problemi (Business Problem)
# 2. Veriyi Anlama (Data Understanding)
# 3. Veriyi Hazırlama (Data Preparation)
# 4. RFM Metriklerinin Hesaplaması (Calculating RFM Metrics)
# 5. RFM Skorlarının Hesaplanması (Calculating RFM Scores)
# 6. RFM Segmentlerinin Oluşturulması ve Analiz Edilmesi (Creating & Analysis RFM Segments)

###############################################################
# 1. İş Problemi (Business Problem)
###############################################################

# Online ayakkabı mağazası olan FLO müşterilerini
# segmentlere ayırıp bu segmentlere göre pazarlama
# stratejileri belirlemek istiyor. Buna yönelik olarak
# müşterilerin davranışları tanımlanacak ve bu
# davranışlardaki öbeklenmelere göre gruplar oluşturulacak.

# Veri Seti Hikayesi

# Veri seti Flo’dan son alışverişlerini 2020 - 2021 yıllarında OmniChannel (hem online hem offline alışveriş yapan)
# olarak yapan müşterilerin geçmiş alışveriş davranışlarından elde edilen bilgilerden oluşmaktadır.

# Değişkenler

# master_id: Eşsiz müşteri numarası
# order_channel: Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
# last_order_channel: En son alışverişin yapıldığı kanal
# first_order_date: Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date: Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online: Müşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline: Müşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online: Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline: Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline: Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online: Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12: Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# 2. Veriyi Anlama (Data Understanding)
###############################################################

import datetime as dt
import pandas as pd

pd.set_option("display.max_columns", None)
#pd.set_option("display.max_rows", None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)

df_= pd.read_csv("C:\\Users\\atama\\PycharmProjects\\CRM_Analytics\\RFM\\FLOMusteriSegmentasyonu\\flo_data_20k.csv")
df = df_.copy()
df.head(10)
df.columns
df.describe()
df.isnull().any()
df.isnull().sum()
df.dtypes

###############################################################
# 3. Veriyi Hazırlama (Data Preparation)
###############################################################

# Müşteri alışveriş sayısı ve ödediği ücret miktarı içeren değişkenleri omnichannel olarak ifade etme
df["omni_num_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["omni_value_total_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
df.head()
df.dtypes
# Tarih Bilgisi Taşıyan Object tipinde değişkenleri Datetime Tipine çevirme
df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])
# çok daha fazla date değişkeni olmasına karşın şu yöntem kullanılabilir
# date_columns = df.columns[df.columns.str.contains("date")]
# df[date_columns] = df[date_columns].apply(pd.to_datetime)
df.info()
#  Alışveriş kanallarındaki müşteri sayısını, toplam alınan ürün sayısı ve toplam harcamaları
df.groupby("order_channel").agg({"master_id":"nunique",
                                 "omni_num_total_ever":"sum",
                                 "omni_value_total_ever":"sum"})

df.sort_values("omni_value_total_ever", ascending=False).head(10)["master_id"]
df.sort_values("omni_num_total_ever", ascending=False)[:10]["master_id"]

# Veri Ön İşleme Aşamasının Fonksiyonlaştırılması
def data_prep(dataframe):
    df["omni_num_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["omni_value_total_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
    date_columns = df.columns[df.columns.str.contains("date")]
    df[date_columns] = df[date_columns].apply(pd.to_datetime)
    return dataframe

data_prep(df)
df.info()

###############################################################
# 4. RFM Metriklerinin Hesaplanması (Calculating RFM Metrics)
###############################################################
#RFM nedir?
#Recency, Frequency, Monetary terimlerinin baş harflerinden oluşur
#Recency: Müşterinin güncelliği yani analizin yapıldığı tarih ile en son alışveriş yapılan tarihin farkıdır
#Frequency: Müşterinin alışveriş sıklığıdır. Yapılan alışveriş sayısını temsil eder.
#Monetary : Müşterinin bıraktığı parasal değerdir.

df["last_order_date"].max()
today_date = dt.datetime(2021,6,1)


df.agg({"last_order_date":lambda x: (today_date - x).days,
        "omni_num_total_ever":lambda x: (x),
        "omni_value_total_ever":lambda x: (x)}).head()

rfm = df.agg({"last_order_date":lambda x: (today_date - x).days,
        "omni_num_total_ever":lambda x: (x),
        "omni_value_total_ever":lambda x: (x)})

rfm.columns = ["recency", "frequency", "monetary"]
rfm["customer_id"] = df["master_id"]
rfm.head()
###############################################################
# 5. RFM Skorlarının Hesaplanması (Calculating RFM Scores)
###############################################################

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5,4,3,2,1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1,2,3,4,5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1,2,3,4,5])
rfm.head()

rfm["RF_SCORE"] = rfm['recency_score'].astype(str) + rfm["frequency_score"].astype(str)


###############################################################
# 6. RFM Segmentlerinin Oluşturulması ve Analiz Edilmesi (Creating & Analysis RFM Segments)
###############################################################

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])
rfm.head()

# a.FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
# tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak
# iletişime geçmek isteniliyor. Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş
# yapan kişiler özel olarak iletişim kurulacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına aktarma.

target_segments_customer_ids = rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
type(target_segments_customer_ids)
cust_ids = df[(df["master_id"].isin(target_segments_customer_ids)) & (df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_ids.to_csv("champion_and_loyal_customers_in_women_categories", index=False)
cust_ids.shape

# b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte
# iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni
# gelen müşteriler özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydedilecektir.

discount_target_ids = rfm[rfm["segment"].isin(["about_to_sleep", "cant_loose", "new_customers"])]["customer_id"]
dicount_cust_ids = df[(df["master_id"].isin(discount_target_ids)) & ((df["interested_in_categories_12"].str.contains("ERKEK")) | (df["interested_in_categories_12"].str.contains("ÇOCUK")))]["master_id"]

dicount_cust_ids.to_csv("discount_customers", index=False)

##########################################################
# Tüm Sürecin Fonksiyonlaştırılması
##########################################################

def create_rfm(dataframe):
     # veriyi hazırlama
     df["omni_num_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
     df["omni_value_total_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
     date_columns = df.columns[df.columns.str.contains("date")]
     df[date_columns] = df[date_columns].apply(pd.to_datetime)

     # RFM Metriklerinin Hesaplanması
     df["last_order_date"].max()
     today_date = dt.datetime(2021, 6, 1)
     rfm = df.agg({"last_order_date": lambda x: (today_date - x).days,
                   "omni_num_total_ever": lambda x: (x),
                   "omni_value_total_ever": lambda x: (x)})

     rfm.columns = ["recency", "frequency", "monetary"]
     rfm["customer_id"] = df["master_id"]

     # RFM Skorlarının oluşturulması
     rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
     rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
     rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])
     rfm["RF_SCORE"] = rfm['recency_score'].astype(str) + rfm["frequency_score"].astype(str)

     # RFM Segmentlerinin Oluşturulması

     seg_map = {
         r'[1-2][1-2]': 'hibernating',
         r'[1-2][3-4]': 'at_Risk',
         r'[1-2]5': 'cant_loose',
         r'3[1-2]': 'about_to_sleep',
         r'33': 'need_attention',
         r'[3-4][4-5]': 'loyal_customers',
         r'41': 'promising',
         r'51': 'new_customers',
         r'[4-5][2-3]': 'potential_loyalists',
         r'5[4-5]': 'champions'
     }

     rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
     # daha düzenli olması adına return kısmına rfm[["customer_id", "recency", "frequency", "monetary", "RF_Score", "segment"]] yazılabilir
     return rfm


rfm_df = create_rfm(df)

rfm_df.head()