const sqlite3 = require("sqlite3").verbose();
const axios = require("axios");
const express = require("express");
const cors = require("cors");
const app = express();
const port = 4000;

app.use(express.json());
app.use(cors());

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});

const DBSOURCE = "db.sqlite";

let db = new sqlite3.Database(DBSOURCE, (err) => {
  if (err) {
    console.log(err.message);
  } else {
    console.log("Connected to the SQLite database.");
    db.run(
      `CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            title TEXT,
            description TEXT,
            price INTEGER,
            category TEXT,
            image TEXT,
            sold INTEGER,
            dateOfSale TEXT
        )`,
      (err) => {
        if (err) {
          console.log(err);
        } else {
          console.log("table created");
        }
      }
    );
  }
});

const fetchAndInsert = async () => {
  const response = await axios.get(
    "https://s3.amazonaws.com/roxiler.com/product_transaction.json"
  );
  const data = response.data;

  for (let item of data) {
    const queryData = `SELECT id FROM transactions WHERE id = ${item.id}`;
    const existingData = await db.get(queryData);
    if (existingData === undefined) {
      const query = `
   INSERT INTO transactions (id, title, price, description, category, image, sold, dateOfSale) 
   VALUES (
       ${item.id},
       '${item.title.replace(/'/g, "''")}',
       ${item.price},
       '${item.description.replace(/'/g, "''")}',
       '${item.category.replace(/'/g, "''")}',
       '${item.image.replace(/'/g, "''")}',
       ${item.sold},
       '${item.dateOfSale.replace(/'/g, "''")}'
   );
`;

      await db.run(query);
    }
  }
  console.log("Transactions added");
};

fetchAndInsert();

app.get("/transactions", (req, res) => {
  let { page = 1, perPage = 10, search = "", month = "" } = req.query;
  if (!month) {
    res.status(400).send("Please provide a month name.");
    return;
  }
  page = page > 0 ? page : 1;
  const offset = (page - 1) * perPage;

  const date = new Date(Date.parse(month + " 1, 2000"));
  let monthNum = date.getMonth() + 1;
  monthNum = monthNum < 10 ? "0" + monthNum : monthNum;
  if (isNaN(monthNum)) {
    return res.status(400).send("invalid month");
  }

  const sql = `
        SELECT * FROM transactions
        WHERE strftime('%m', dateOfSale) = '${monthNum}' AND (title LIKE '%${search}%' OR description LIKE '%${search}%' OR price LIKE '%${parseFloat(
    search
  )}%')
        ORDER BY id ASC
        LIMIT ${perPage} OFFSET ${offset}
    `;

  db.all(sql, (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json({
      message: "success",
      data: rows,
      pagination: {
        page: page,
        perPage: perPage,
      },
    });
  });
});

app.get("/statistics", (req, res) => {
  const { month } = req.query;
  if (!month) {
    return res.status(400).send("Please provide a month name.");
  }

  const date = new Date(Date.parse(month + " 1, 2000"));
  let monthNum = date.getMonth() + 1;
  monthNum = monthNum < 10 ? "0" + monthNum : monthNum;
  if (isNaN(monthNum)) {
    return res.status(400).send("invalid month");
  }

  const sql = `
        SELECT 
            SUM(price) AS TotalSalesAmount,
            COUNT(CASE WHEN sold = 1 THEN 1 END) AS TotalSoldItems,
            COUNT(CASE WHEN sold = 0 THEN 1 END) AS TotalNotSoldItems
        FROM transactions
        WHERE 
        strftime('%m', dateOfSale) = '${monthNum}'
    `;

  db.get(sql, (err, result) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json({
      totalSalesAmount: result.TotalSalesAmount,
      totalSoldItems: result.TotalSoldItems,
      totalNotSoldItems: result.TotalNotSoldItems,
    });
  });
});

app.get("/items-in-price-range", (req, res) => {
  const { month } = req.query;
  if (!month) {
    return res.status(400).send("Please provide a month name.");
  }
  const date = new Date(Date.parse(month + " 1, 2000"));
  let monthNum = date.getMonth() + 1;
  monthNum = monthNum < 10 ? "0" + monthNum : monthNum;
  if (isNaN(monthNum)) {
    res.status(400).send("invalid month");
    return;
  }

  const sql = `
        SELECT
            COUNT(CASE WHEN price BETWEEN 0 AND 100 THEN 1 END) AS '0-100',
            COUNT(CASE WHEN price BETWEEN 101 AND 200 THEN 1 END) AS '101-200',
            COUNT(CASE WHEN price BETWEEN 201 AND 300 THEN 1 END) AS '201-300',
            COUNT(CASE WHEN price BETWEEN 301 AND 400 THEN 1 END) AS '301-400',
            COUNT(CASE WHEN price BETWEEN 401 AND 500 THEN 1 END) AS '401-500',
            COUNT(CASE WHEN price BETWEEN 501 AND 600 THEN 1 END) AS '501-600',
            COUNT(CASE WHEN price BETWEEN 601 AND 700 THEN 1 END) AS '601-700',
            COUNT(CASE WHEN price BETWEEN 701 AND 800 THEN 1 END) AS '701-800',
            COUNT(CASE WHEN price BETWEEN 801 AND 900 THEN 1 END) AS '801-900',
            COUNT(CASE WHEN price >= 901 THEN 1 END) AS '901-above'
        FROM transactions
        WHERE strftime('%m', dateOfSale) = '${monthNum}'
    `;

  db.get(sql, (err, result) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json({
      month: monthNum,
      data: result,
    });
  });
});

app.get("/items-in-each-category", (req, res) => {
  const { month } = req.query;
  if (!month) {
    return res.status(400).send("Please provide a month name.");
  }
  const date = new Date(Date.parse(month + " 1, 2000"));
  let monthNum = date.getMonth() + 1;
  monthNum = monthNum < 10 ? "0" + monthNum : monthNum;
  if (isNaN(monthNum)) {
    res.status(400).send("invalid month");
    return;
  }

  const sql = `
    SELECT category, COUNT(*) AS count
    FROM transactions
    WHERE strftime('%m', dateOfSale) = '${monthNum}'
    GROUP BY category
    `;

  db.all(sql, (err, rows) => {
    if (err) {
      return res.status(500).json({ error: err.message });
    }
    res.json({
      month: monthNum,
      data: rows,
    });
  });
});

app.get("/combined-data", async (req, res) => {
  const { month } = req.query;
  if (!month) {
    return res.status(400).send("Please provide a month name.");
  }
  const date = new Date(Date.parse(month + " 1, 2000"));
  let monthNum = date.getMonth() + 1;
  monthNum = monthNum < 10 ? "0" + monthNum : monthNum;
  if (isNaN(monthNum)) {
    res.status(400).send("invalid month");
    return;
  }

  async function fetchData(sql) {
    return new Promise((resolve, reject) => {
      db.all(sql, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      });
    });
  }
  try {
    const statisticsQuery = `
        SELECT 
            SUM(price) AS TotalSalesAmount,
            COUNT(CASE WHEN sold = 1 THEN 1 END) AS TotalSoldItems,
            COUNT(CASE WHEN sold = 0 THEN 1 END) AS TotalNotSoldItems
        FROM transactions
        WHERE 
            strftime('%m', dateOfSale) = '${monthNum}'`;

    const itemsInPriceRangeQuery = `
        SELECT
            COUNT(CASE WHEN price BETWEEN 0 AND 100 THEN 1 END) AS '0-100',
            COUNT(CASE WHEN price BETWEEN 101 AND 200 THEN 1 END) AS '101-200',
            COUNT(CASE WHEN price BETWEEN 201 AND 300 THEN 1 END) AS '201-300',
            COUNT(CASE WHEN price BETWEEN 301 AND 400 THEN 1 END) AS '301-400',
            COUNT(CASE WHEN price BETWEEN 401 AND 500 THEN 1 END) AS '401-500',
            COUNT(CASE WHEN price BETWEEN 501 AND 600 THEN 1 END) AS '501-600',
            COUNT(CASE WHEN price BETWEEN 601 AND 700 THEN 1 END) AS '601-700',
            COUNT(CASE WHEN price BETWEEN 701 AND 800 THEN 1 END) AS '701-800',
            COUNT(CASE WHEN price BETWEEN 801 AND 900 THEN 1 END) AS '801-900',
            COUNT(CASE WHEN price >= 901 THEN 1 END) AS '901-above'
        FROM transactions
        WHERE strftime('%m', dateOfSale) = '${monthNum}'`;

    const itemsInCategoriesQuery = `
        SELECT category, COUNT(*) AS count
        FROM transactions
        WHERE strftime('%m', dateOfSale) = '${monthNum}'
        GROUP BY category`;

    const statisticsData = await fetchData(statisticsQuery);
    const itemsInPriceRangeData = await fetchData(itemsInPriceRangeQuery);
    const itemsInCategoriesData = await fetchData(itemsInCategoriesQuery);
    res.json({
      month: month.toUpperCase(),
      statistics: statisticsData,
      itemsInPriceRange: itemsInPriceRangeData,
      itemsInEachCategory: itemsInCategoriesData,
    });
  } catch (err) {
    console.error("Error fetching data:", err);
    res.status(500).send({ error: err.message });
  }
});
