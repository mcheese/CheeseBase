#include "Simple-Web-Server/server_http.hpp"
#include <cheesebase.h>
#include <model/json_print.h>

using Server = SimpleWeb::Server<SimpleWeb::HTTP>;

const std::vector<std::string> examples {
  "SELECT ELEMENT x FROM [1,2,3,4,5,6,7,8] AS x WHERE x % 2 = 0",

  "SELECT x AS nr FROM [1,2,3,4] AS x",

  "SELECT ELEMENT [x,y] FROM { a:1, b:2, c:3 } AS { x:y }",

  R"(SELECT ELEMENT  { "odd": odd, "sum": SUM(x), "group": group }
          FROM  numbers AS x
      GROUP BY  x % 2 AS odd)",
R"(    SELECT  o.product AS product,
            o.amount  AS amount,
            s.name    AS supplier

      FROM  orders AS o
INNER JOIN  suppliers AS s
        ON  s.id == o.supplier)"

};

const std::string getExFuncs() {
  std::string output;
  size_t nr = 1;
  for (auto& e : examples) {
    output += "function ex" + std::to_string(nr++) +
              "() { "
              "document.getElementById(\"query-text\").value = `" +
              e + "`; }";
  }
  return output;
}

const std::string getExButtons() {
  std::string output;
  for (size_t nr = 1; nr <= examples.size(); nr++) {
    output += "<button class=\"example\" type=\"button\" onclick=\"ex" +
              std::to_string(nr) + "()\">Example  " + std::to_string(nr) +
              "</button>\n";
  }
  return output;
}

const std::string kCSS = R"(
html {
  font-family: sans-serif;
}

.container {
  width: 70%;
  min-width: 600px;
  margin-left: auto;
  margin-right: auto;
}

.left {
  width: 20%;
  min-width: 160px;
  float: left;
}

.right {
  width: 80%;
  float: left;
}

h1 {
  font-weight: 500;
  text-align: center;
}

h3 {
  font-weight: 500;
  margin-top: 20px;
  margin-bottom: 4px;
}

textarea {
  width: 100%;
  height: 300px;
  border: 1px solid #ccc;
  border-radius: 4px;
  padding: 6px 12px;
  color: #555;
  font: 14px/normal 'Hack', 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', 'source-code-pro', monospace;
  resize: none;
}

button {
  background-image: linear-gradient(to bottom,#f5f5f5 0,#e8e8e8 100%);
  color: #555;
  border-radius: 4px;
  border: solid 1px #ccc;
  cursor: pointer;
  font-weight: 500;
  font-size: 14px;
  height: 40px;
}

button.example {
  width: 80%;
}

button.exec {
  width: 120px;
}

.button {
  text-align: center;
}

pre {
    font: 14px/normal 'Hack', 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', 'source-code-pro', monospace;
    background-color: #eee;
    padding: 10px;
    color: #555;
    border: solid 1px #ccc;
    border-radius: 4px;
}

)";

const std::string kJScript = R"(
function exec() {
  var textarea = document.getElementById("query-text");
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState === 4 && xhttp.status === 200) {
      document.getElementById("query-result").innerHTML = xhttp.responseText;
    }
  };

  xhttp.open("POST", "query", true);
  xhttp.setRequestHeader("Content-type", "text/html");
  xhttp.send(textarea.value);
}
)" + getExFuncs();

const std::string kPage = R"(
<html>
<head>
<style>
)" + kCSS + R"x(
</style>
</head>
<body>
<h1>Cheesebase - SQL++ Query Engine</h1>
<hr>
<div class="container">
  <div class="row">
    <div class="left">
      <h3>Examples:</h3>
)x" + getExButtons() + R"x(
    </div>
    <div class="right">
      <h3>Query:</h3>
      <textarea id="query-text"></textarea>
      <div class="button">
        <button class="exec" type="button" onclick="exec()">Run Query</button>
      </div>
      <h3>Result:</h3>
      <pre id="query-result"></pre>
    </div>
  </div>
</div>
<script>
)x" + kJScript + R"(
</script>
</body>
</html>
)";

int main(int argc, char** argv) {
  if (argc < 2) { 
    std::cout << "Usage: webserver <DB-file>\n";
    return 1;
  }

  Server s(1337, 1);
  cheesebase::CheeseBase cb{ argv[1] };

  s.resource["^/query$"]["POST"] = [&cb](auto res, auto req) {
    auto content = req->content.string();

    std::cout << "--------[ query ]--\n" << content << "\n";

    std::ostringstream ss;

    try {
      cheesebase::model::JsonPrinter{ ss }(cb.query(content));
    } catch (std::exception& e) {
      ss << "Error: " << e.what();
    }

    const auto str = ss.str();
    *res << "HTTP/1.1 200 OK\r\nContent-Length: " << str.size() << "\r\n\r\n"
         << str;
  };

  s.resource["^/$"]["GET"] = [](auto res, auto req) {
    auto content = req->content.string();

    std::cout << "--------[ default ]--\n" << content << "\n";

    *res << "HTTP/1.1 200 OK\r\nContent-Length: " << kPage.size() << "\r\n\r\n"
         << kPage;
  };

  s.start();
}
