# Report: RAG Complaint Chatbot (Task 1 & Task 2)

## Task 1: Exploratory Data Analysis & Data Preprocessing

**Objective**  
Prepare the CFPB complaints dataset for the RAG system by cleaning, filtering, and normalizing the text data.

**Key Generic Steps**
1.  **Data Loading**: Loaded raw data from `../data/raw/complaints.csv`.
2.  **Initial Analysis**:
    *   Checked dataset shape and missing values.
    *   Found significant number of missing "Consumer complaint narrative" entries.
    *   Visualized distribution of product categories.

**Data Filtering & Normalization**
*   **Target Products**: Focused on four main financial product categories:
    *   Credit card
    *   Personal loan
    *   Savings account
    *   Money transfer
*   **Normalization**: Mapped various sub-products (e.g., "Student loan", "Vehicle loan") into the main categories.
*   **Missing Data**: Removed rows where the consumer narrative was missing.

**Text Cleaning**
Implemented a text cleaning pipeline to standardize the narratives:
*   Lowercasing all text.
*   **Boilerplate Removal**: Removed common phrases like "I am writing to file a complaint" and "To whom it may concern".
*   **Noise Reduction**: Removed special characters, keeping only alphanumeric characters and spaces.
*   **Whitespace**: Normalized multiple spaces to single spaces.

**Outcome**
The cleaned and filtered dataset was saved to `../data/processed/filtered_complaints.csv`.

---

## Task 2: Chunking, Embedding & Vector Store Creation

**Objective**
Build a searchable vector knowledge base (Vector Store) to enable semantic search for the RAG pipeline.

**Sampling**
*   **Stratified Sampling**: Due to the large dataset size, a representative sample of **12,000 complaints** was selected.
*   **Proportion Maintenance**: Used stratified sampling to ensure the distribution of product categories in the sample matched the original filtered dataset.

**Text Chunking**
*   **Tool**: `RecursiveCharacterTextSplitter` from LangChain.
*   **Parameters**:
    *   `chunk_size`: 500 characters.
    *   `chunk_overlap`: 50 characters (to maintain context between chunks).
*   **Process**: Split the detailed complaint narratives into smaller, manageable text chunks.

**Embeddings**
*   **Model**: `sentence-transformers/all-MiniLM-L6-v2`.
*   **Process**: Generated high-dimensional vector representations (embeddings) for all text chunks.

**Vector Store (ChromaDB)**
*   **Tool**: ChromaDB.
*   **Collection**: Created a collection named `complaint_chunks`.
*   **Ingestion**: Stored the text chunks, their corresponding embeddings, and metadata (Complaint ID, Product Category).
*   **Storage**: Persisted the database to `../vector_store/chroma`.

**Outcome**
A fully populated vector database ready for similarity search queries.
