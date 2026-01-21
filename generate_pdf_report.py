from fpdf import FPDF

class PDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 15)
        self.cell(0, 10, 'RAG Complaint Chatbot Report', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

    def chapter_title(self, title):
        self.set_font('Helvetica', 'B', 12)
        self.set_fill_color(200, 220, 255)
        self.cell(0, 6, title, 0, 1, 'L', fill=True)
        self.ln(4)

    def chapter_body(self, body):
        self.set_font('Helvetica', '', 11)
        self.multi_cell(0, 5, body)
        self.ln()

    def bullet_point(self, text):
        self.set_font('Helvetica', '', 11)
        self.cell(5) # Indent
        self.cell(5, 5, chr(149), 0, 0) # Bullet
        self.multi_cell(0, 5, text)
        self.ln(2)

pdf = PDF()
pdf.add_page()

# Task 1
pdf.chapter_title('Task 1: Exploratory Data Analysis & Data Preprocessing')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Objective:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'Prepare the CFPB complaints dataset for the RAG system by cleaning, filtering, and normalizing the text data.')
pdf.ln()

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Key Steps:', 0, 1)
pdf.ln(2)

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '1. Data Loading & Initial Analysis', 0, 1)
pdf.bullet_point('Loaded raw data from ../data/raw/complaints.csv.')
pdf.bullet_point('Identified significant missing values in consumer narratives.')
pdf.bullet_point('Visualized distribution of product categories.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '2. Data Filtering & Normalization', 0, 1)
pdf.bullet_point('Filtered for target products: Credit card, Personal loan, Savings account, Money transfer.')
pdf.bullet_point('Normalized sub-products (e.g., "Student loan") into main categories.')
pdf.bullet_point('Removed rows with missing narratives.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '3. Text Cleaning', 0, 1)
pdf.bullet_point('Converted text to lowercase.')
pdf.bullet_point('Removed boilerplate phrases (e.g., "I am writing to file a complaint").')
pdf.bullet_point('Removed special characters and normalized whitespace.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Outcome:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'Saved cleaned dataset to ../data/processed/filtered_complaints.csv.')
pdf.ln()


# Task 2
pdf.chapter_title('Task 2: Chunking, Embedding & Vector Store Creation')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Objective:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'Build a searchable vector knowledge base (Vector Store) to enable semantic search.')
pdf.ln()

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Key Steps:', 0, 1)
pdf.ln(2)

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '1. Sampling', 0, 1)
pdf.bullet_point('Performed stratified sampling to select 12,000 complaints.')
pdf.bullet_point('Maintained original category proportions.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '2. Text Chunking', 0, 1)
pdf.bullet_point('Used RecursiveCharacterTextSplitter.')
pdf.bullet_point('Chunk size: 500 characters, Overlap: 50 characters.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '3. Embeddings', 0, 1)
pdf.bullet_point('Model: sentence-transformers/all-MiniLM-L6-v2.')
pdf.bullet_point('Generated vector embeddings for all chunks.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '4. ChromaDB Vector Store', 0, 1)
pdf.bullet_point('Created "complaint_chunks" collection.')
pdf.bullet_point('Stored chunks, embeddings, and metadata (ID, Product).')
pdf.bullet_point('Persisted database to ../vector_store/chroma.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Outcome:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'A populated vector database ready for semantic search.')
pdf.ln()

# Task 3
pdf.chapter_title('Task 3: RAG Pipeline Implementation')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Objective:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'Implement the Retrieval-Augmented Generation (RAG) system to answer user queries based on the complaint knowledge base.')
pdf.ln()

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Key Components:', 0, 1)
pdf.ln(2)

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '1. Retriever (Vector Store Manager)', 0, 1)
pdf.bullet_point('Integrated with ChromaDB for efficient similarity search.')
pdf.bullet_point('Retrieves top-k relevant complaint chunks based on query embeddings.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '2. Generator (LLM)', 0, 1)
pdf.bullet_point('Utilized Hugging Face Transformers pipeline.')
pdf.bullet_point('Model: GPT-2 (configurable for other Causal LM models).')
pdf.bullet_point('Generates natural language answers using the retrieved context.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '3. Prompt Engineering', 0, 1)
pdf.bullet_point('Designed a context-aware prompt template.')
pdf.bullet_point('Instructs the LLM to act as a financial assistant and cite evidence.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Outcome:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'A functional RAG pipeline capable of semantic retrieval and answer generation.')
pdf.ln()

# Task 4
pdf.chapter_title('Task 4: RAG Pipeline Evaluation')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Objective:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'Assess the performance and relevance of the RAG system using a standard set of test questions.')
pdf.ln()

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Key Steps:', 0, 1)
pdf.ln(2)

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '1. Test Dataset', 0, 1)
pdf.bullet_point('Defined a set of representative questions (e.g., "Why are people unhappy with credit cards?").')
pdf.bullet_point('Focused on different product categories and issue types.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '2. Evaluation Process', 0, 1)
pdf.bullet_point('Initialized the RAG pipeline with the persisted vector store.')
pdf.bullet_point('Queried the pipeline for each test question.')
pdf.bullet_point('Retrieved top-k documents and generated answers.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, '3. Metrics & Reporting', 0, 1)
pdf.bullet_point('Captured generated answers and retrieved source chunks.')
pdf.bullet_point('Generated a detailed markdown report (evaluation_report.md) for qualitative analysis.')

pdf.set_font('Helvetica', 'B', 11)
pdf.cell(0, 5, 'Outcome:', 0, 1)
pdf.set_font('Helvetica', '', 11)
pdf.multi_cell(0, 5, 'Verified that the system retrieves relevant context and generates coherent responses for domain-specific queries.')
pdf.ln()

output_path = "report_task1_task2_task3_task4.pdf"
pdf.output(output_path)
print(f"PDF generated: {output_path}")
