import express from "express";
// Create a new express app instance
const app: express.Application = express();
const PORT = 3000;
app.get("/hello", function (req, res) {
  res.send("Hello World!");
});
app.listen(PORT, "127.0.0.1", function () {
  console.log("App is listening on port ${PORT}!");
});
