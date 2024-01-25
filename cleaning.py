# First attempt to clean German text
stop_words = set(stopwords.words('german'))

# Create a German lemmatizer
stemmer = SnowballStemmer("german")

# Add additional stopwords to the set
additional_stopwords = {'mehr', 'wurde','heute','deutsch','ab','soll', 'seit','neu','wurden', 'mehrere','mehreren'}
stop_words.update(additional_stopwords)
print(stop_words)

def clean_text(text):
    # Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)

    # Convert to lowercase
    text = text.lower()

    # Remove numbers
    text = re.sub(r'\d+', '', text)

    # Tokenize the text
    words = word_tokenize(text)

    # Lemmatize and remove stopwords
    words = [stemmer.stem(word) for word in words if word not in stop_words]

    # Join the words back into a clean text
    clean_text = ' '.join(words)

    return clean_text

#remark to myself: there is something off with stopwords, I am not happy with them
