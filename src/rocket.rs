#[macro_use] extern crate rocket;
use rocket::fs::TempFile;

#[post("/upload", format = "plain", data = "<file>")]
async fn upload(mut file: TempFile<'_>) -> std::io::Result<()> {
    file.persist_to("data/file.txt").await
}

#[launch]
fn rocket() -> _ {
    rocket::build().mount("/", routes![upload])
}