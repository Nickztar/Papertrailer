use csv::{ReaderBuilder};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::{Index, doc, Score, DocAddress};
use tantivy::schema::{Schema, TEXT, STORED};

//Schema:
// id ?
// generated_at date
// received_at date
// source_id i32
// source_name str
// source_ip str
// facility_name str
// severity_name str
// program str
// message str


fn main() {
    let mut schema_builder = Schema::builder();
    let date_field = schema_builder.add_text_field("date", TEXT | STORED);
    let machine_field = schema_builder.add_text_field("machine", TEXT | STORED);
    let identifier_field = schema_builder.add_text_field("id", TEXT | STORED);
    let body_field = schema_builder.add_text_field("body", TEXT | STORED);
    let schema = schema_builder.build();

    let index = Index::create_from_tempdir(schema.clone()).unwrap();

    let mut index_writer = index.writer(100_000_000).unwrap();
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'\t')
        .from_path("test.tsv").unwrap();
    for record in rdr.records()
    {
        match record {
            Ok(rec) => {
                let mut rec_iter = rec.iter();
                let date = rec_iter.next().unwrap_or("todo");
                let machine = rec_iter.next().unwrap_or("todo");
                let id = rec_iter.next().unwrap_or("todo");
                let body = rec_iter.fold("".to_string(), | acc, item| {
                    acc + item
                });
                index_writer.add_document(doc!(
                    date_field => date,
                    machine_field => machine,
                    identifier_field => id,
                    body_field => body
                )).unwrap();
            },
            Err(err) => println!("{:?}", err),
        }
    }
    index_writer.commit().unwrap();
    let reader = index.reader().unwrap();

    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![body_field]);
    let query = query_parser.parse_query("*").unwrap();
    let top_docs: Vec<(Score, DocAddress)> =
    searcher.search(&query, &TopDocs::with_limit(10)).unwrap();

    for (_score, doc_address) in top_docs {
        // Retrieve the actual content of documents given its `doc_address`.
        let retrieved_doc = searcher.doc(doc_address).unwrap();
        println!("{}", schema.to_json(&retrieved_doc));
    }
}
