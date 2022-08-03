use std::{time::Instant, str::{from_utf8}};

use key_value_db::Database;

fn main() {
    let instant = Instant::now();

    let small_string = get_string(38);
    let medium_string = get_string(100);
    let large_string = get_string(200);
    let very_large_string = get_string(4100);

    let strings = [&small_string, &medium_string, &large_string, &small_string, &medium_string];

    println!("Strings allocated: {:?}", instant.elapsed().as_secs_f64());

    let mut db = Database::new("test.db").unwrap();
    db.set("key1", &small_string);
    db.set("key2", &medium_string);
    db.set("key3", &large_string);

    let mut buffer = [0; 200];

    println!("Strings stored: {:?}", instant.elapsed().as_secs_f64());

    let iterations = 10_000_000;

    (0..iterations).into_iter().for_each(|_| {
        db.get_to_buffer("key1", &mut buffer);
        db.get_to_buffer("key2", &mut buffer);
        db.get_to_buffer("key3", &mut buffer);
    });

    println!("Strings read: {:?}, iterations: {:?}", instant.elapsed().as_secs_f64(), iterations);
    println!("Result strings:");

    for key in ["key1", "key2", "key3"] {
        println!("{:?}", from_utf8(&db.get(key).unwrap()).unwrap());
    }
}

fn get_string(length: i32) -> Vec<u8> {
    (0..length).map(|i| (i % 10).to_string()).collect::<Vec<String>>().join("").as_bytes().to_vec()
}