import os
import re
import json
import logging
from typing import Union, Tuple, List
from pathlib import Path
import filetype  # type: ignore
import torch
from torchvision.transforms import Compose, Resize, CenterCrop, ToTensor  # type: ignore
from PIL import Image  # type: ignore
from sentence_transformers import SentenceTransformer  # type: ignore
import hnswlib  # type: ignore
import PyPDF2  # type: ignore
import textract  # type: ignore
import wordninja  # type: ignore
from ftfy import fix_text  # type: ignore
from .persistor import Persistor
from .utils import download_url
from .tokenizer import SimpleTokenizer

logging.disable(logging.INFO)


class CLIPVectorizer:
    """ Multimodal vector space for images and texts powered by OpenAI's CLIP """

    def __init__(self, verbose=False):
        self.verbose = verbose
        TOKENIZER_URL = "https://openaipublic.azureedge.net/clip/bpe_simple_vocab_16e6.txt.gz"
        MODEL_URL = "https://openaipublic.azureedge.net/clip/models/ \
            40d365715913c9da98579312b702a82c18be219cc2a73407c4526f58eba950af/ViT-B-32.pt"
        db_path = Path.home() / ".config/hypertag/"
        clip_files_path = db_path / "CLIP-files"
        os.makedirs(clip_files_path, exist_ok=True)
        tokenizer_name = TOKENIZER_URL.split("/")[-1]
        if not Path(clip_files_path / tokenizer_name).is_file():
            print("Downloading tokenizer...")
            download_url(TOKENIZER_URL, clip_files_path / tokenizer_name)
        model_name = "model.pt"
        if not Path(clip_files_path / model_name).is_file():
            print("Downloading CLIP model...")
            download_url(MODEL_URL, clip_files_path / model_name)

        self.model = torch.jit.load(str(clip_files_path / model_name)).cuda().eval()
        self.tokenizer = SimpleTokenizer(bpe_path=str(clip_files_path / tokenizer_name))
        input_resolution = self.model.input_resolution.item()
        self.preprocess = Compose(
            [
                Resize(input_resolution, interpolation=Image.BICUBIC),
                CenterCrop(input_resolution),
                ToTensor(),
            ]
        )
        self.image_mean = torch.tensor([0.48145466, 0.4578275, 0.40821073]).cuda()
        self.image_std = torch.tensor([0.26862954, 0.26130258, 0.27577711]).cuda()

        # Build or load index
        corpus_vectors, corpus_paths = self.get_image_corpus()
        index_dir = Path.home() / ".config/hypertag/index-files/"
        self.index_path = index_dir / "images.index"
        os.makedirs(index_dir, exist_ok=True)
        self.index = hnswlib.Index(space="cosine", dim=512)

        if self.index_path.exists():
            if self.verbose:
                print("Loading image index...")
            self.index.load_index(str(self.index_path), max_elements=len(corpus_vectors))
            self.update_index(len(corpus_vectors))
        else:
            # Create the HNSWLIB index
            if not corpus_vectors:
                return
            if self.verbose:
                print("Creating HNSWLIB image index...")
            self.index.init_index(max_elements=len(corpus_vectors), ef_construction=400, M=64)
            # Train the index to find a suitable clustering
            self.index.add_items(corpus_vectors, list(range(len(corpus_vectors))))
            if self.verbose:
                print("Saving index to:", self.index_path)
            self.index.save_index(str(self.index_path))
            # Update DB (set files as indexed)
            with Persistor() as db:
                db.set_indexed_by_file_paths(corpus_paths)
        # Controlling the recall by setting ef (lower is faster but more inaccuare)
        self.index.set_ef(50)  # ef should always be > top_k_hits

    def update_index(self, current_corpus_length):
        # Handle new vectorized elements
        with Persistor() as db:
            file_paths = db.get_unindexed_file_paths()
            compatible_files = get_image_files(file_paths)
            corpus = db.get_file_embedding_vectors(compatible_files)
        new_corpus_paths = []
        new_corpus_vectors = []
        for doc_path, embedding_vector in corpus:
            new_corpus_vectors.append(json.loads(embedding_vector))
            new_corpus_paths.append(doc_path)
        len_new = len(new_corpus_vectors)
        len_old = current_corpus_length - 1
        new_total_size = len_old + len_new
        if self.verbose:
            print("NEW UNINDEXED FILES:", len_new)
        if new_corpus_vectors:
            # Add unindexed vectors to index
            self.index.resize_index(new_total_size)
            print(list(range(len_old, new_total_size)))
            self.index.add_items(
                new_corpus_vectors,
                list(range(len_old, new_total_size)),
            )
            if self.verbose:
                print("Saving updated index to:", self.index_path)
            self.index.save_index(str(self.index_path))
            # Update DB (set files as indexed)
            with Persistor() as db:
                db.set_indexed_by_file_paths(new_corpus_paths)

    def get_image_corpus(self):
        # Retrieve vectorized image vectors and paths
        with Persistor() as db:
            file_paths = db.get_vectorized_file_paths()
            compatible_files = get_image_files(file_paths)
            corpus = db.get_file_embedding_vectors(compatible_files)

        corpus_paths = []
        corpus_vectors = []
        for doc_path, embedding_vector in corpus:
            corpus_vectors.append(json.loads(embedding_vector))
            corpus_paths.append(doc_path)
        return corpus_vectors, corpus_paths

    def search_image(self, text_query: str, path, top_k, score):
        query_vector = None
        # Check if text_query is an image file path
        file_path = Path(text_query)
        if file_path.exists() and file_path.is_file():
            file_type_guess = filetype.guess(str(file_path))
            if file_type_guess and file_type_guess.extension in {"jpg", "png"}:
                query_vector = self.encode_image(str(file_path))

        corpus_vectors, corpus_paths = self.get_image_corpus()
        image_features = torch.Tensor(corpus_vectors)
        image_features /= image_features.norm(dim=-1, keepdim=True)

        # Encode text query
        if query_vector is None:
            query_vector = self.encode_text(text_query)
        query_vector /= query_vector.norm(dim=-1, keepdim=True)
        # Indexed top-k nearest neighbor query
        corpus_ids, distances = self.index.knn_query(query_vector.cpu(), k=top_k)
        # Print results
        results = []
        for corpus_id, score_value in zip(corpus_ids[0], distances[0]):
            match_file_path = str(corpus_paths[corpus_id])
            file_name = match_file_path.split("/")[-1]
            if path:
                file_name = match_file_path
            if score:
                result = f"{file_name} ({score_value:.4f})"
                results.append(result)
                print(result)
            else:
                results.append(file_name)
                print(file_name)
        return results

    def encode_image(self, path: str):
        image = Image.open(path).convert("RGB")
        image = self.preprocess(image)
        image.unsqueeze_(0)
        image_input = image.cuda()
        image_input -= self.image_mean[:, None, None]
        image_input /= self.image_std[:, None, None]
        with torch.no_grad():
            image_features = self.model.encode_image(image_input).float()
        return image_features

    def encode_text(self, text: str):
        sot_token = self.tokenizer.encoder["<|startoftext|>"]
        eot_token = self.tokenizer.encoder["<|endoftext|>"]
        text_token = self.tokenizer.encode(text)
        text_tokens = [[sot_token] + text_token + [eot_token]]
        text_input = torch.zeros(len(text_tokens), self.model.context_length, dtype=torch.long)

        for i, tokens in enumerate(text_tokens):
            text_input[i, : len(tokens)] = torch.tensor(tokens)

        text_input = text_input.cuda()

        with torch.no_grad():
            text_features = self.model.encode_text(text_input).float()
        return text_features


class TextVectorizer:
    def __init__(self, verbose=False):
        self.verbose = verbose
        os.environ["TOKENIZERS_PARALLELISM"] = "true"
        model_name = "stsb-distilbert-base"
        self.model = SentenceTransformer(model_name)

        # Build or load index
        corpus_vectors, corpus_paths = self.get_text_corpus()
        index_dir = Path.home() / ".config/hypertag/index-files/"
        self.index_path = index_dir / "texts.index"
        os.makedirs(index_dir, exist_ok=True)
        self.index = hnswlib.Index(space="cosine", dim=768)

        if self.index_path.exists():
            if self.verbose:
                print("Loading text index...")
            self.index.load_index(str(self.index_path), max_elements=len(corpus_vectors))
            self.update_index(len(corpus_vectors))
        else:
            # Create the HNSWLIB index
            if not corpus_vectors:
                return
            if self.verbose:
                print("Creating HNSWLIB text index...")
            self.index.init_index(max_elements=len(corpus_vectors), ef_construction=400, M=64)
            # Then we train the index to find a suitable clustering
            self.index.add_items(corpus_vectors, list(range(len(corpus_vectors))))
            if self.verbose:
                print("Saving index to:", self.index_path)
            self.index.save_index(str(self.index_path))
            # Update DB (set files as indexed)
            with Persistor() as db:
                db.set_indexed_by_file_paths(corpus_paths)
        # Controlling the recall by setting ef (lower is faster but more inaccuare)
        self.index.set_ef(50)  # ef should always be > top_k_hits

    def update_index(self, current_corpus_length):
        # Handle new vectorized but unindexed elements
        with Persistor() as db:
            file_paths = db.get_unindexed_file_paths()
            compatible_files = [path for path, _file_type in get_text_documents(file_paths)]
            corpus = db.get_file_embedding_vectors(compatible_files)
        new_corpus_paths = []
        new_corpus_vectors = []
        for doc_path, embedding_vector in corpus:
            new_corpus_vectors.append(json.loads(embedding_vector))
            new_corpus_paths.append(doc_path)
        len_new = len(new_corpus_vectors)
        len_old = current_corpus_length - 1
        new_total_size = len_old + len_new
        if self.verbose:
            print("NEW UNINDEXED FILES:", len_new)
        if new_corpus_vectors:
            # Add unindexed vectors to index
            self.index.resize_index(new_total_size)
            print(list(range(len_old, new_total_size)))
            self.index.add_items(
                new_corpus_vectors,
                list(range(len_old, new_total_size)),
            )
            if self.verbose:
                print("Saving updated index to:", self.index_path)
            self.index.save_index(str(self.index_path))
            # Update DB (set files as indexed)
            with Persistor() as db:
                db.set_indexed_by_file_paths(new_corpus_paths)

    def get_text_corpus(self):
        # Returns text paths and embedding vectors
        with Persistor() as db:
            text_files = db.get_files(show_path=True)
        text_document_tuples = get_text_documents(text_files)
        text_document_paths = [path for path, _file_type in text_document_tuples]
        with Persistor() as db:
            corpus = db.get_file_embedding_vectors(text_document_paths)
        corpus_paths = []
        corpus_vectors = []
        for doc_path, embedding_vector in corpus:
            corpus_vectors.append(json.loads(embedding_vector))
            corpus_paths.append(doc_path)
        return corpus_vectors, corpus_paths

    def compute_text_embedding(self, sentences: List[List[str]]) -> List[float]:
        for i, s in enumerate(sentences):
            if len(s) == 1:
                sentences[i] = 2 * sentences[i]  # Needed for transformer model
        sentence_vectors = self.model.encode(sentences, show_progress_bar=False)
        if sentence_vectors.shape[0] > 0:
            return torch.Tensor(sentence_vectors.mean(axis=0)).tolist()
        else:
            return torch.Tensor([sentence_vectors]).tolist()

    def search(self, text_query: str, path=False, top_k=10, score=False):
        """ Execute a semantic search that returns best matching text documents """
        # Parse query: duplicate words marked with * (increases search weight)
        parsed_query = []
        rex = re.compile(r"\*+\w+")
        args = re.findall(r"\*+'.*?'|\*+\".*?\"|\S+", text_query)

        for w in args:
            if w.startswith("*"):
                w = w.replace('"', "").replace("'", "")
                matches = rex.search(w)
                if matches:
                    replicate_n = matches.group().count("*")
                    for _ in range(replicate_n + 1):
                        parsed_query.append(w[replicate_n:])
            else:
                parsed_query.append(w)
        parsed_text_query = " ".join(parsed_query)
        corpus_vectors, corpus_paths = self.get_text_corpus()

        sentence_query = clean_transform(parsed_text_query, 0, 1)
        query_vector = self.compute_text_embedding(sentence_query)
        # Indexed top-k nearest neighbor query
        corpus_ids, distances = self.index.knn_query(query_vector, k=top_k)

        results = []
        for corpus_id, score_value in zip(corpus_ids[0], distances[0]):
            file_path = corpus_paths[corpus_id]
            file_name = file_path.split("/")[-1]
            if path:
                file_name = file_path
            if score:
                result = f"{file_name} ({score_value:.4f})"
                results.append(result)
                print(result)
            else:
                results.append(file_name)
                print(file_name)
        return results


def get_image_files(file_paths, verbose=False):
    # Keep only images (JPG)
    doc_types = {"jpg", "png"}
    if verbose:
        print("Supported image file types:", doc_types)
    compatible_files = []
    for file_path in file_paths:
        file_type_guess = filetype.guess(str(file_path))
        if file_type_guess is None:
            continue
        file_type = file_type_guess.extension.lower()
        if file_type in doc_types:
            compatible_files.append(str(file_path))
    return compatible_files


def get_text_documents(file_paths: List[str], verbose=False) -> List[Tuple[str, str]]:
    # TODO: Add markdown support
    doc_types = {
        "pdf",
        "epub",
        "doc",
        "docx",
        "html",
        "json",
        "rtf",
        "txt",
        "xls",
        "xlsx",
        "ps",
        "pptx",
        "odt",
        "eml",
        "msg",
    }
    if verbose:
        print("Supported text file types:", doc_types)
    # Keep only text files
    compatible_files = []
    for file_path in file_paths:
        file_type_guess = filetype.guess(str(file_path))
        if file_type_guess is None:
            file_name = file_path.split("/")[-1]
            name_parts = file_name.split(".")
            if len(name_parts) > 1 and len(name_parts[-1]) < 6:
                file_type = name_parts[-1]
            else:
                continue
        else:
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
