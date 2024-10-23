import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import unicodedata
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')

# Initialize the lemmatizer
lemmatizer = WordNetLemmatizer()

def normalize_text(text):
    # Normalize unicode characters to standard form
    normalized_text = unicodedata.normalize('NFKD', text)
    # Filter out non-ASCII characters
    ascii_text = ''.join([c for c in normalized_text if ord(c) < 128])
    return ascii_text

def preprocess_text(text):
    text = normalize_text(text)
    # Tokenize the text
    tokens = word_tokenize(text)

    # Convert to lowercase
    tokens = [word.lower() for word in tokens]

    # Remove stop words
    stop_words = set(stopwords.words('english'))

    negation_words = {'not', 'no', 'nor', 'never', 'none', 'nobody', 'nothing', 'neither', 'nowhere'}
    stop_words = stop_words - negation_words
    tokens = [word for word in tokens if word not in stop_words]

    # # Lemmatize the tokens
    # tokens = [lemmatizer.lemmatize(word) for word in tokens]

    # Reconstruct the sentence
    processed_text = ' '.join(tokens)
    return processed_text