package main

import (
"fmt"
"log"
)

func main() {
db, err := Open("my.db")
if err != nil {
log.Fatal(err)
}
defer db.Close()

if err := db.Set([]byte("emmanuel"), []byte("systems_engineer")); err != nil {
log.Fatal(err)
}

val, err := db.Get([]byte("emmanuel"))
if err != nil {
log.Fatal(err)
}
fmt.Printf("emmanuel → %s\n", val)

if err := db.Delete([]byte("emmanuel")); err != nil {
log.Fatal(err)
}

_, err = db.Get([]byte("emmanuel"))
fmt.Println(err)
}
