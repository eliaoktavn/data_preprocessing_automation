import nltk
nltk.download('stopwords')
nltk.download('wordnet')
from nltk.tokenize import word_tokenize
import streamlit as st
import numpy as np
import pickle
from tensorflow.keras.models import load_model
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import re
nltk.download('stopwords') 
nltk.download('punkt')
nltk.download('wordnet')

model = load_model('model_improved')

# Initialize lemmatizer
lemmatizer = WordNetLemmatizer()

# make variable for additional stop word
additional_stopwords = ['unk','com', 'ti', 'game', 'db', 'bf', 'c go', 'unk', 'p', 't co', 'm', 've', 'a a', 's', 'u', 're', 'tt', 're', 'co', 'u']

# Define Stopwords
stpwds_en = list(set(stopwords.words('english')))

def expand_contractions(sentence):
    #change certain word
    sentence = re.sub(r"can't", "can not",sentence)
    sentence = re.sub(r"y'all", "you all",sentence)
    sentence = re.sub(r"wanna", " want to",sentence)
    sentence = re.sub(r"im", "i am",sentence)
    sentence = re.sub(r"gotta", " got to",sentence)
    sentence = re.sub(r"go ta", " got to",sentence)
    # Tokenize the sentence into words
    tokens = word_tokenize(sentence)
    # Define contraction mapping
    contractions_mapping = {"n't": " not", "'s": " is", "'m": " am", "'re": " are", "'ll": " will", "'ve": " have", "'d": " would", "got ta": " got to"}
    # Expand contractions
    expanded_tokens = [contractions_mapping.get(token, token) for token in tokens]
    # Join the tokens back into a sentence
    expanded_sentence = ' '.join(expanded_tokens)
    return expanded_sentence
prediction = []
def preprocess_text(text):
    # Expand contractions
    text = expand_contractions(text)

    # Case folding
    text = text.lower()

    # Mention removal
    text = re.sub("@[A-Za-z0-9_]+", " ", text)
    text = re.sub("@ [A-Za-z0-9_]+", " ", text)

    # Hashtags removal
    text = re.sub("#[A-Za-z0-9_]+", " ", text)

    # Newline removal (\n)
    text = re.sub(r"\\n", " ",text)

    # Whitespace removal
    text = text.strip()

    # URL removal
    text = re.sub(r"http\S+", " ", text)
    text = re.sub(r"www.\S+", " ", text)
    text = re.sub(r"twitch.tv\S+", " ", text)
    text = re.sub(r"twitch tv\S+", " ", text)
    text = re.sub(r"pic.twitter.com\S+", " ", text)
    text = re.sub(r"dlvr.it\S+", " ", text)
    text = re.sub(r"dfr.it / RMTrgF", " ", text)
    text = re.sub(r"dlvr.it\S+", " ", text)
    text = re.sub(r"dlvr.it \S+", " ", text)


    # Non-letter removal (such as emoticon, symbol (like μ, $, 兀), etc
    text = re.sub("[^A-Za-z\s']", " ", text)

    # Tokenization
    tokens = word_tokenize(text)

    # Stopwords removal
    tokens = [word for word in tokens if word not in stpwds_en ]
    tokens = [word for word in tokens if word not in additional_stopwords]

    # Lemmatizing
    tokens = [lemmatizer.lemmatize(word) for word in tokens]

    # Combining Tokens
    text = ' '.join(tokens)
    return text

def predict(text):
    # Preprocess the input text
    preprocessed_text = preprocess_text(text)

    # Predict the probabilities
    predictions_proba = model.predict(preprocessed_text.toarray())


    # Get class with maximum probability
    prediction = np.argmax(predictions_proba, axis=-1)

    return prediction

# Assuming a multi-class classification with one-hot encoded labels
category_sentiment = {0: 'Irrelevant', 1: 'Negative', 2: 'Neutral', 3:'Positive'}


def run():
    # Streamlit app setup
    st.title("Text Input for Prediction")

    user_input = st.text_area("Enter your text here:")

    if st.button('Predict'):
        predicted_sentiment = predict(user_input)
        
        # Display the predicted category
        st.write(f'Predicted Category: {category_sentiment[prediction]}')

if __name__ == '__main__':
    run()