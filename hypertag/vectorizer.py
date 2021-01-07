import os
import re
import json
from typing import Union, Tuple, List
from pathlib import Path
import filetype  # type: ignore
import torch
from sentence_transformers import SentenceTransformer  # type: ignore
from sentence_transformers.util import semantic_search  # type: ignore
import PyPDF2  # type: ignore
import textract  # type: ignore
import wordninja  # type: ignore
from ftfy import fix_text  # type: ignore
from .persistor import Persistor


class Vectorizer:
    def __init__(self):
        os.environ["TOKENIZERS_PARALLELISM"] = "true"
        self.model_name = "stsb-distilbert-base"  # "average_word_embeddings_glove.6B.300d"
        self.model = SentenceTransformer(self.model_name)

    def compute_text_embedding(self, sentences: List[List[str]]) -> List[float]:
        if self.model_name == "stsb-distilbert-base":
            for i, s in enumerate(sentences):
                if len(s) == 1:
                    sentences[i] = 2 * sentences[i]  # Needed for transformer model
            sentence_strings = sentences
        else:
            sentence_strings = [" ".join(s) for s in sentences]  # For glove (slows transformers)
        sentence_vectors = self.model.encode(sentence_strings, show_progress_bar=False)
        if sentence_vectors.shape[0] > 0:
            return torch.Tensor(sentence_vectors.mean(axis=0)).tolist()
        else:
            return torch.Tensor([sentence_vectors]).tolist()

    def vector_search(self, query_vector: List[float], corpus_embeddings: List[List[float]], top_k):
        return semantic_search(
            torch.Tensor(query_vector), torch.Tensor(corpus_embeddings), top_k=top_k
        )

    def search(self, text_query: str, path=False, top_k=10, score=False):
        """ Execute a semantic search that returns best matching text documents """
        # Parse query: duplicate words marked with * (increases search weight)
        parsed_query = []
        rex = re.compile(r"\*+\w+")
        args = re.findall(r"\*+'.*?'|\*+\".*?\"|\S+", text_query)

        for w in args:
            if w.startswith("*"):
                w = w.replace('"', "").replace("'", "")
                replicate_n = rex.search(w).group().count("*")
                for _ in range(replicate_n + 1):
                    parsed_query.append(w[replicate_n:])
            else:
                parsed_query.append(w)
        parsed_text_query = " ".join(parsed_query)

        with Persistor() as db:
            text_files = db.get_files(show_path=True)
        text_document_tuples = get_text_documents(text_files)
        text_document_paths = [path for path, _file_type in text_document_tuples]
        with Persistor() as db:
            corpus = db.get_file_embedding_vectors(text_document_paths)
        if len(corpus) == 0:
            print("No relevant files indexed...")
            return
        sentence_query = clean_transform(parsed_text_query, 0, 1)
        query_vector = self.compute_text_embedding(sentence_query)
        corpus_paths = []
        corpus_vectors = []
        for doc_path, embedding_vector in corpus:
            corpus_vectors.append(json.loads(embedding_vector))
            corpus_paths.append(doc_path)

        top_matches = self.vector_search(query_vector, corpus_vectors, top_k=top_k)
        results = []
        for match in top_matches[0]:
            corpus_id, score_value = match["corpus_id"], match["score"]
            file_path = corpus_paths[corpus_id]
            file_name = file_path.split("/")[-1]
            if path:
                file_name = file_path
            if score:
                result = f"{file_name} ({score_value})"
                results.append(result)
                print(result)
            else:
                results.append(file_name)
                print(file_name)
        return results


def get_text_documents(file_paths: List[str]) -> List[Tuple[str, str]]:
    with Persistor() as db:
        doc_id = db.get_tag_id_by_name("Documents")
        doc_types = set()
        for tag_id, _type_name in db.get_tag_id_children_ids_names(doc_id):
            doc_types.add(db.get_tag_name_by_id(tag_id))

    # Keep only text files
    compatible_files = []
    for file_path in file_paths:
        file_type_guess = filetype.guess(str(file_path))
        if file_type_guess is None:
            continue
        file_type = file_type_guess.extension.lower()
        if file_type in doc_types:
            compatible_files.append((str(file_path), file_type))
    return compatible_files


def extract_clean_text(args: Tuple[str, str, bool, int, int]) -> Tuple[str, List[List[str]]]:
    file_path, file_type, cache, min_words, min_word_length = args
    if cache:
        with Persistor() as db:
            # Use saved cleaned text if available (cache)
            # TODO: Evaluate thoroughly if storing all tokens is worth it
            # Hunch: Yes, we can store text tokens and a single mapping (token : word)
            sentences_string = db.get_clean_text_of_file(file_path)
            if sentences_string is not None:
                sentences = [s.split(" ") for s in json.loads(sentences_string)]
                return str(file_path), sentences

    text = extract_text(file_path, file_type)
    sentences = []
    if text:
        sentences = clean_transform(text, min_words, min_word_length)
        if cache:
            with Persistor() as db:
                # Save cleaned text
                db.add_clean_text_to_file(file_path, json.dumps([" ".join(s) for s in sentences]))
    else:
        with Persistor() as db:
            # Mark as not parseable
            print("NOT PARSEABLE")
            db.add_clean_text_to_file(file_path, json.dumps([]))
    return str(file_path), sentences


def extract_text(file_path_: str, file_type: str) -> str:
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
    text = fix_text(text, normalization="NFKC")
    text = text.replace("\n", " ").replace("\t", " ")
    text = re.sub("[^A-Za-z0-9 .']+", "", text)  # Remove all special chars but space . '
    sentences = [s for s in text.split(".") if s]

    ninja_sentences = []
    for s in sentences:
        ninja_sentence = []
        for w in s.split(" "):
            if len(w) > 5:
                for nw in wordninja.split(w):  # Split non space seperated word groups up
                    if len(nw) > min_word_length:
                        ninja_sentence.append(nw)
            else:
                if len(w) > min_word_length:
                    ninja_sentence.append(w)
        ninja_sentences.append(ninja_sentence)
    # Remove duplicates
    ninja_sentence_set = {" ".join(s) for s in ninja_sentences}

    long_sentences = [s.split(" ") for s in ninja_sentence_set if len(s.split(" ")) > min_words]
    return long_sentences


def get_pdf_text(file_path: Union[Path, str]) -> str:
    text = ""
    try:
        text = textract.process(file_path, encoding="utf8").decode("utf-8")
    except Exception:
        try:
            text = get_pypdf_text(file_path)
        except Exception as ex:
            print("Exception while parsing:", ex)
    if not text:
        print("Falling back to Tesseract OCR parsing (get a tea...) for:", file_path)
        try:
            text = textract.process(file_path, method="tesseract").decode("utf-8")
        except Exception as ex:
            print("OCR failed:", ex)
            text = ""
    return text


def get_pypdf_text(file_path: Union[Path, str]) -> str:
    text = ""
    parsed = 0
    failed = 0
    with open(str(file_path), mode="rb") as f:
        reader = PyPDF2.PdfFileReader(f, strict=False)
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
