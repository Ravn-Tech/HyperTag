import os
import re
import json
from multiprocessing import Pool
from typing import Union, Tuple, List
from pathlib import Path
import filetype
from tqdm import tqdm
import torch
from sentence_transformers import SentenceTransformer  # type: ignore
from sentence_transformers.util import semantic_search  # type: ignore
from .persistor import Persistor


os.environ["TOKENIZERS_PARALLELISM"] = "true"
model_name = "average_word_embeddings_glove.6B.300d"  # "stsb-distilbert-base" (slower)
model = SentenceTransformer(model_name)  # TODO: Optimize only load *once* in daemon


def index(rebuild=False, cache=False, cores: int = 0):
    """ Vectorize text files (needed for semantic search) """
    # TODO: index images
    # TODO: auto index on file addition (import)
    print("Vectorizing text documents...")
    cuda = torch.cuda.is_available()
    if cuda:
        print("Using CUDA to speed stuff up")
    else:
        print("CUDA not available (this might take a while)")
    if cache:
        print("Caching cleaned texts (database will grow big)")
    with Persistor() as db:
        if rebuild:
            print("Rebuilding index")
            file_paths = db.get_indexed_file_paths()
        else:
            file_paths = db.get_unindexed_file_paths()
    i = 0
    compatible_files = get_text_documents(file_paths)
    min_words = 5
    min_word_length = 4
    args = []
    for file_path, file_type in compatible_files:
        args.append((file_path, file_type, cache, min_words, min_word_length))
    inference_tuples = []

    # Preprocess using multi-processing (default uses all available cores)
    if cores <= 0:
        n_cores = os.cpu_count()
    else:
        n_cores = cores
    pool = Pool(processes=n_cores)
    print(f"Preprocessing texts using {n_cores} cores...")
    with tqdm(total=len(compatible_files)) as t:
        for file_path, sentences in pool.imap_unordered(extract_clean_text, args):
            t.update(1)
            if sentences:
                inference_tuples.append((file_path, sentences))
    print(f"Cleaned {len(inference_tuples)} text docs successfully")
    print("Starting inference...")
    # Compute embeddings
    for file_path, sentences in tqdm(inference_tuples):
        document_vector = compute_text_embedding(sentences)
        if (
            document_vector is not None
            and type(document_vector) is list
            and len(document_vector) > 0
        ):
            with Persistor() as db:
                db.add_file_embedding_vector(file_path, json.dumps(document_vector))
                db.conn.commit()
            i += 1
        else:
            print(type(document_vector))
            print("Failed to parse file - skipping:", file_path)
    print(f"Vectorized {str(i)} file/s successfully")


def search(text_query: str, path=False, top_k=10, score=False):
    """ Execute a semantic search that returns best matching text documents """
    with Persistor() as db:
        text_files = db.get_files(show_path=True)
    text_document_tuples = get_text_documents(text_files)
    text_document_paths = [path for path, _file_type in text_document_tuples]
    with Persistor() as db:
        corpus = db.get_file_embedding_vectors(text_document_paths)
    if len(corpus) == 0:
        print("No relevant files indexed...")
        return
    sentence_query = clean_transform(text_query, 0, 1)
    query_vector = compute_text_embedding(sentence_query)
    corpus_paths = []
    corpus_vectors = []
    for doc_path, embedding_vector in corpus:
        corpus_vectors.append(json.loads(embedding_vector))
        corpus_paths.append(doc_path)

    top_matches = vector_search(query_vector, corpus_vectors, top_k=top_k)
    for match in top_matches[0]:
        corpus_id, score_value = match["corpus_id"], match["score"]
        file_path = corpus_paths[corpus_id]
        file_name = file_path.split("/")[-1]
        if path:
            file_name = file_path
        if score:
            print(file_name, f"({score_value})")
        else:
            print(file_name)


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
