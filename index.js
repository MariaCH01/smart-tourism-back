require('dotenv').config(); 
const serverless = require("serverless-http");
const express = require("express");
const mysql = require('mysql2');
const cors = require('cors');
const app = express();
app.use(cors());


const pool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,     
  password: process.env.MYSQL_PASSWORD, 
  database: process.env.MYSQL_DB  
});

const promisePool = pool.promise();

app.get('/get_categories', async (req, res) => {
    try {
      const categories = req.query.categories || '';
  
      let sqlQuery;
  
      if (categories === '') {
        sqlQuery = `
          SELECT sc.name as subcategory, c.name as category
          FROM subcategories sc
          JOIN categories c ON c.id = sc.category_id;
        `;
      } else {
        if (categories === 'ALL') {
          sqlQuery = `
            SELECT sc.name as subcategory, c.name as category
            FROM subcategories sc
            JOIN categories c ON c.id = sc.category_id;
          `;
        } else {
          const categoriesList = categories.split(';').map(cat => promisePool.escape(cat)).join(', ');
          sqlQuery = `
            SELECT sc.name as subcategory, c.name as category
            FROM subcategories sc
            JOIN categories c ON c.id = sc.category_id
            WHERE c.name IN (${categoriesList});
          `;
        }
      }
  
      const [results] = await promisePool.query(sqlQuery);
  
      // Procesamiento de los resultados
      const resultRows = results;
  
      if (categories === '' || categories === 'ALL') {
        const categoriesMap = new Map();
        resultRows.forEach(row => {
          if (!categoriesMap.has(row.category)) {
            categoriesMap.set(row.category, { category: row.category, subcategories: [], n: 0 });
          }
          const catData = categoriesMap.get(row.category);
          catData.subcategories.push(row.subcategory);
          catData.n++;
        });
  
        const resultDict = Array.from(categoriesMap.values());
        const nresults = resultDict.reduce((sum, catData) => sum + catData.n, 0);
  
        res.json({
          data: resultDict,
          ntotal: nresults,
          message: 'ok'
        });
      } else {
        const categories = [...new Set(resultRows.map(row => row.category))];
        const scategories = [...new Set(resultRows.map(row => row.subcategory))];
        const nresults = scategories.length;
  
        res.json({
          data: [{ category: categories, n: nresults, subcategories: scategories }],
          ntotal: nresults,
          message: 'ok'
        });
      }
    } catch (e) {
      console.error(`Error: ${e}`);
      res.json({
        message: `error: ${e.message}`
      });
    }
  });

  app.get('/get_places', async (req, res) => {
    try {
      const categories = req.query.categories || 'ALL';
      const subcategories = req.query.subcategories || 'ALL';
  
      let sqlQuery;
  
      if (categories === 'ALL') {
        if (subcategories === 'ALL') {
          sqlQuery = `
            SELECT r.nombre, c.name as category, s.name as subcategory
            FROM restaurants r
            JOIN restaurant_category rc ON r.id = rc.restaurant_id
            JOIN categories c ON rc.category_id = c.id
            JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
            JOIN subcategories s ON rs.subcategory_id = s.id;
          `;
        } else {
          const subcategoriesList = subcategories.split(';').map(cat => promisePool.escape(cat)).join(', ');
          sqlQuery = `
            SELECT r.nombre, c.name as category, s.name as subcategory
            FROM restaurants r
            JOIN restaurant_category rc ON r.id = rc.restaurant_id
            JOIN categories c ON rc.category_id = c.id
            JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
            JOIN subcategories s ON rs.subcategory_id = s.id
            WHERE s.name IN (${subcategoriesList});
          `;
        }
      } else {
        const categoriesList = categories.split(';').map(cat => promisePool.escape(cat)).join(', ');
  
        if (subcategories === 'ALL') {
          sqlQuery = `
            SELECT r.nombre, c.name as category, s.name as subcategory
            FROM restaurants r
            JOIN restaurant_category rc ON r.id = rc.restaurant_id
            JOIN categories c ON rc.category_id = c.id
            JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
            JOIN subcategories s ON rs.subcategory_id = s.id
            WHERE c.name IN (${categoriesList});
          `;
        } else {
          const subcategoriesList = subcategories.split(';').map(cat => promisePool.escape(cat)).join(', ');
          sqlQuery = `
            SELECT r.nombre, c.name as category, s.name as subcategory
            FROM restaurants r
            JOIN restaurant_category rc ON r.id = rc.restaurant_id
            JOIN categories c ON rc.category_id = c.id
            JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
            JOIN subcategories s ON rs.subcategory_id = s.id
            WHERE c.name IN (${categoriesList}) AND s.name IN (${subcategoriesList});
          `;
        }
      }
  
      const [results] = await promisePool.query(sqlQuery);
  
      // Procesamiento de los resultados
      const resultRows = results;
      const places = [...new Set(resultRows.map(row => row.nombre))];
  
      const resultDict = [{ places: places }];
  
      res.json({
        data: resultDict,
        ntotal: places.length,
        message: 'ok'
      });
  
    } catch (e) {
      console.error(`Error: ${e}`);
      res.json({
        message: `error: ${e.message}`
      });
    }
  });

  app.get('/get_info_place', async (req, res) => {
    try {
      const places = req.query.places || 'ALL';
      const categories = req.query.categories || 'ALL';
      const subcategories = req.query.subcategories || 'ALL';
  
      let sqlQuery = `
        SELECT r.nombre, c.name as category, s.name as subcategory, r.latitud, r.longitud, r.descripcion, r.pais, r.municipio, r.departamento, r.direccion, r.telefono, r.email, r.url, r.red_social, r.image, r.resena, r.hora_apertura, r.menu, r.destacado
        FROM restaurants r
        JOIN restaurant_category rc ON r.id = rc.restaurant_id
        JOIN categories c ON rc.category_id = c.id
        JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
        JOIN subcategories s ON rs.subcategory_id = s.id
      `;
  
      let whereClauses = [];
  
      if (categories !== 'ALL') {
        const categoriesList = categories.split(';').map(cat => promisePool.escape(cat)).join(', ');
        whereClauses.push(`c.name IN (${categoriesList})`);
      }
  
      if (subcategories !== 'ALL') {
        const subcategoriesList = subcategories.split(';').map(cat => promisePool.escape(cat)).join(', ');
        whereClauses.push(`s.name IN (${subcategoriesList})`);
      }
  
      if (places !== 'ALL') {
        const placesList = places.split(';').map(place => promisePool.escape(place)).join(', ');
        whereClauses.push(`r.nombre IN (${placesList})`);
      }
  
      if (whereClauses.length > 0) {
        sqlQuery += " WHERE " + whereClauses.join(' AND ');
      }
  
      sqlQuery += ";";
  
      const [results] = await promisePool.query(sqlQuery);
  
      // Procesamiento de los resultados
      const resultRows = results;
      const df = resultRows.map(row => ({
        name: row.nombre,
        category: row.category,
        subcategory: row.subcategory,
        latitude: parseFloat(row.latitud),
        longitude: parseFloat(row.longitud),
        description: row.descripcion,
        country: row.pais,
        department: row.departamento,
        municipality: row.municipio,
        address: row.direccion,
        phone: row.telefono,
        email: row.email,
        url: row.url,
        social_media: row.red_social,
        image: row.image,
        review: row.resena,
        opening_hour: row.hora_apertura,
        menu: row.menu,
        featured: row.destacado,
        md: row.md
      }));
  
      const resultDict = df.map(row => ({
        display_name: row.name,
        category: row.category,
        subcategories: row.subcategory,
        lat: row.latitude,
        lon: row.longitude,
        desc: row.description,
        country: row.country,
        department: row.department,
        municipality: row.municipality,
        address: row.address,
        phone: row.phone,
        email: row.email,
        url: row.url,
        red_social: row.social_media,
        image: row.image,
        review: row.review,
        open_hour: row.opening_hour,
        menu: row.menu,
        featured: row.featured,
        md: row.md
      }));
  
      res.json({
        data: resultDict,
        ntotal: resultDict.length,
        message: 'ok'
      });
  
    } catch (e) {
      console.error(`Error: ${e}`);
      res.json({
        message: `error: ${e.message}`
      });
    }
  });


module.exports.handler = serverless(app);

