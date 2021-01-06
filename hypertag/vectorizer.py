import os
import re
import json
from typing import Union, Tuple, List
from pathlib import Path
import torch
from sentence_transformers import SentenceTransformer  # type: ignore
from sentence_transformers.util import semantic_search  # type: ignore
from .persistor import Persistor


os.environ["TOKENIZERS_PARALLELISM"] = "true"
model_name = "average_word_embeddings_glove.6B.300d"  # "stsb-distilbert-base" (slower)
model = SentenceTransformer(model_name)  # TODO: Optimize only load *once* in daemon


def vector_search(query_vector: List[float], corpus_embeddings: List[List[float]], top_k):
    return semantic_search(torch.Tensor(query_vector), torch.Tensor(corpus_embeddings), top_k=top_k)


def extract_clean_text(args: Tuple[str, str, bool, int, int]) -> Tuple[str, List[List[str]]]:
    file_path, file_type, cache, min_words, min_word_length = args
    if cache:
        with Persistor() as db:
            # Use saved cleaned text if available (cache)
            # TODO: Evaluate thoroughly if storing all tokens is worth it
            # Hunch: Yes, cuz we can store text tokens and a single mapping (token : word)
            text = db.get_add_clean_text_of_file(file_path)
            if text:
                return str(file_path), json.loads(text)

    text = extract_text(file_path, file_type)
    sentences = []
    if text:
        sentences = clean_transform(text, min_words, min_word_length)
        if cache:
            with Persistor() as db:
                # Save cleaned text
                db.add_clean_text_to_file(file_path, json.dumps([" ".join(s) for s in sentences]))

    return str(file_path), sentences


def extract_text(file_path_: str, file_type: str) -> str:
    import textract  # type: ignore

    file_path = Path(file_path_)
    # print(f"Parsing: {file_path} as type: {file_type}")
    if file_type == "pdf":
        text = get_pdf_text(file_path)
    else:
        try:
            text = str(textract.process(file_path))
        except Exception:
            # print("Exception while parsing:", ex)
            text = ""  # Failed to parse...
    return text


def clean_transform(text: str, min_words: int, min_word_length: int) -> List[List[str]]:
    import wordninja  # type: ignore
    from ftfy import fix_text  # type: ignore

    text = fix_text(text, normalization="NFKC")
    sentences = [s for s in text.split(".") if s]

    nss = []
    for s in sentences:
        if len(s.split(" ")) < 3 and len(s) > 42:
            # Break apart long non-space seperated sequences by . and \n
            subs = []
            for ss in re.split(r"\. |\n", s):
                subs.append(ss)
            nss.append(" ".join(subs))
        else:
            nss.append(s)

    sentences = nss

    ninja_sentences = []
    for s in sentences:
        ninja_sentences.append([w for w in wordninja.split(s) if len(w) > min_word_length])
    # Remove duplicates
    ninja_sentence_set = {" ".join(s) for s in ninja_sentences}

    long_sentences = [s.split(" ") for s in ninja_sentence_set if len(s.split(" ")) > min_words]
    return long_sentences


def get_pdf_text(file_path: Union[Path, str]) -> str:
    import textract  # type: ignore

    try:
        text = str(textract.process(file_path))
    except Exception:
        try:
            text = get_pypdf_text(file_path)
            if not text:
                # print("Falling back to Tesseract parsing...")
                text = str(textract.process(file_path, method="tesseract"))
        except Exception as ex:
            print("Exception while parsing:", ex)
            return ""  # Failed to parse...
    return text


def get_pypdf_text(file_path: Union[Path, str]) -> str:
    import PyPDF2  # type: ignore

    text = ""
    parsed = 0
    failed = 0
    with open(str(file_path), mode="rb") as f:
        reader = PyPDF2.PdfFileReader(f)
        for page in reader.pages:
            try:
                if failed > 42:
                    print("Stopping to parse after 42 failed pages...")
                    return ""
                parsed += 1
                text += " " + page.extractText()
            except Exception:
                # print("failed to parse page", parsed)
                failed += 1
    return text


def compute_text_embedding(sentences: List[List[str]]) -> List[float]:
    # Needed for glove model (slows down transformers -> remove when using)
    sentence_strings = [" ".join(s) for s in sentences]
    sentence_vectors = model.encode(sentence_strings, show_progress_bar=False)
    if sentence_vectors.shape[0] > 0:
        return torch.Tensor(sentence_vectors.mean(axis=0)).tolist()
    else:
        return torch.Tensor([sentence_vectors]).tolist()
