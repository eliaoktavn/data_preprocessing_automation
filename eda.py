# eda.py

import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
from PIL import Image

st.set_page_config(
    page_title = "Sentiment Analtysis in Game Sector"
)

def run():
    # Membuat judul
    st.set_option('deprecation.showPyplotGlobalUse', False)
    st.title("Analisis Sentiment pada Tweet terkait Game")

    # Menambah gambar
    image = Image.open("game.jpg")
    st.image(image, caption = "caption: ilustrasi orang bermain game")


    st.write("# Eksplorasi Data Sentiment pada Twitter")
    st.write("Page ini ditampilkan visualisasi data pada Analisis Sentimen secara general pada beberapa top played game. ")
    
    st.markdown("---")

    st.subheader("Informasi Data Tweet")
    st.write("Berikut tampilan DataFrame berisi informasi tweet yang diunggah pada Platform Twitter")
    df = pd.read_csv('twitter_trainvalid.csv', header=None, names=["id","entity","sentiment","tweet"])
    st.dataframe(df)

    st.markdown("---")
    
    st.write("#### Sentiment Percentage")
    st.write("Berikut tampilan persentase sentiment yang ada pada data")
    sentiment_counts = df['sentiment'].value_counts()
    plt.figure(figsize=(8, 6))
    plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', colors=['skyblue', 'orange', 'lightgreen', 'lightcoral'])
    plt.title('Sentiment Distribution')
    plt.show()
    st.pyplot()
    st.write("Didapat bahwa persentase sentimen cukup merata dengan sentiment paling banyak adalah Negative.")
   
    st.markdown("---")
    
    st.write("#### General Sentiment Tweet")
    image = Image.open("wordcloud_final.png")
    st.image(image, caption = "caption: wordcloud setelah dilakukan pembersihan")
    st.write("Diketahui decara general kat ayang mendominasi adalah .....")
 
    st.markdown("---")

    st.write("#### Positive Sentiment Tweet")
    image = Image.open("wordcloud_positive.png")
    st.image(image, caption = "caption: wordcloud setelah dilakukan pembersihan")
    st.write("Diketahui pada sentimen positive kata yang mendominasi adalah .....")
    
    
    st.write("#### Negative Sentiment Tweet")
    image = Image.open("wordcloud_negative.png")
    st.image(image, caption = "caption: wordcloud setelah dilakukan pembersihan")
    st.write("Diketahui pada sentimen negative kata yang mendominasi adalah .....")
    
    
    st.write("#### Neutral Sentiment Tweet")
    image = Image.open("wordcloud_neutral.png")
    st.image(image, caption = "caption: wordcloud setelah dilakukan pembersihan")
    st.write("Diketahui pada sentimen neutral kata yang mendominasi adalah .....")
    
    
    st.write("#### Irrelevant Sentiment Tweet")
    image = Image.open("wordcloud_irrelevant.png")
    st.image(image, caption = "caption: wordcloud setelah dilakukan pembersihan")
    st.write("Diketahui pada sentimen irrelevant kata yang mendominasi adalah .....")
    
if __name__ == "__main__":
    run()