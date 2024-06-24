from flask import Flask, jsonify, request, Response
from flask_mysqldb import MySQL
from config import config
import pandas as pd
import json

app = Flask(__name__)

connection = MySQL(app)

@app.route('/get_categories', methods=['GET'])
def get_categories():
    try:
        cursor = connection.connection.cursor()
        categories = request.args.get('categories', '')

        if categories == '':
            
            sql_query = """
                SELECT sc.name as subcategory, c.name as category
                FROM subcategories sc
                JOIN categories c ON c.id = sc.category_id;
            """
            
            cursor.execute(sql_query)
            result = cursor.fetchall()
            
            df = pd.DataFrame(result, columns=['scat', 'gcat'])
            
            gcategories = df.groupby('gcat').agg(
                scat=("scat", lambda x: ';'.join(map(str, x))),
                ncat=("scat", 'nunique')
                ).reset_index()

            gcategories['dict'] = gcategories.apply(lambda x: {'category': x['gcat'], 'subcategories':x['scat'].split(";"), 'n': x['ncat']}, axis=1)
            
            result_dict = list(gcategories['dict'])
            nresults = sum(gcategories['ncat'])
            
        else:
            
            if categories == 'ALL':

                sql_query = """
                SELECT sc.name as subcategory, c.name as category
                FROM subcategories sc
                JOIN categories c ON c.id = sc.category_id;
                """
                
                
            else:
                
                categories_q = ", ".join([f"'{i}'" for i in categories.split(';')])
                
                sql_query = f"""
                SELECT sc.name as subcategory, c.name as category
                FROM subcategories sc
                JOIN categories c ON c.id = sc.category_id
                WHERE c.name IN ({categories_q});
                """
                
            cursor.execute(sql_query)
            result = cursor.fetchall()
            
            df = pd.DataFrame(result, columns=['scat', 'gcat'])

            categories = list(df['gcat'].unique()) if categories == 'ALL' else categories
            scategories = list(df['scat'].unique())
            nresults = len(scategories)
            
            result_dict = [{'category':categories, 'n':nresults, 'subcategories':scategories}]

        response = {
        'data': result_dict,
        'ntotal': nresults,
        'message': 'ok'
        }

        return Response(json.dumps(response, ensure_ascii=False), mimetype='application/json')

    except Exception as e:
        print(f"Error: {e}")
        response = {
        'message': f"error: {e}"
        }
        
        return Response(json.dumps(response, ensure_ascii=False), mimetype='application/json')

@app.route('/get_places', methods=['GET'])
def get_places():
    try:
        cursor = connection.connection.cursor()
        categories = request.args.get('categories', 'ALL')
        subcategories = request.args.get('subcategories', 'ALL')

        if categories == 'ALL':
            
            if subcategories == 'ALL':
                
                sql_query = """
                SELECT r.nombre, c.name as category, s.name as subcategory
                FROM restaurants r
                JOIN restaurant_category rc ON r.id = rc.restaurant_id
                JOIN categories c ON rc.category_id = c.id
                JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
                JOIN subcategories s ON rs.subcategory_id = s.id;
                """
                
            else:
                subcategories_q = ", ".join([f"'{i}'" for i in subcategories.split(';')])
                sql_query = f"""
                SELECT r.nombre, c.name as category, s.name as subcategory
                FROM restaurants r
                JOIN restaurant_category rc ON r.id = rc.restaurant_id
                JOIN categories c ON rc.category_id = c.id
                JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
                JOIN subcategories s ON rs.subcategory_id = s.id
                WHERE s.name IN ({subcategories_q});
                """
        else:
            categories_q = ", ".join([f"'{i}'" for i in categories.split(';')])
            if subcategories == 'ALL':
                sql_query = f"""
                SELECT r.nombre, c.name as category, s.name as subcategory
                FROM restaurants r
                JOIN restaurant_category rc ON r.id = rc.restaurant_id
                JOIN categories c ON rc.category_id = c.id
                JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
                JOIN subcategories s ON rs.subcategory_id = s.id
                WHERE c.name IN ({categories_q});
                """
            else:
                subcategories_q = ", ".join([f"'{i}'" for i in subcategories.split(';')])
                sql_query = f"""
                SELECT r.nombre, c.name as category, s.name as subcategory
                FROM restaurants r
                JOIN restaurant_category rc ON r.id = rc.restaurant_id
                JOIN categories c ON rc.category_id = c.id
                JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
                JOIN subcategories s ON rs.subcategory_id = s.id
                WHERE c.name IN ({categories_q}) AND s.name IN ({subcategories_q});
                """

        cursor.execute(sql_query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=['name', 'category', 'subcategory'])

        places = list(df['name'].unique())

        result_dict = [{'places': places}]
        
        response = {
        'data': result_dict,
        'ntotal': len(places),
        'message': 'ok'
        }

        return Response(json.dumps(response, ensure_ascii=False), mimetype='application/json')

    except Exception as e:
        print(f"Error: {e}")
        response = {
        'message': f"error: {e}"
        }
        
        return Response(json.dumps(response, ensure_ascii=False), mimetype='application/json')


@app.route('/get_info_place', methods=['GET'])
def get_info_place():
    try:
        cursor = connection.connection.cursor()
        places = request.args.get('places', 'ALL')
        categories = request.args.get('categories', 'ALL')
        subcategories = request.args.get('subcategories', 'ALL')

        sql_query = """
        SELECT r.nombre, c.name as category, s.name as subcategory, r.latitud, r.longitud, r.descripcion, r.pais, r.municipio, r.departamento, r.direccion, r.telefono, r.email, r.url, r.red_social, r.image, r.resena, r.hora_apertura, r.menu, r.destacado
        FROM restaurants r
        JOIN restaurant_category rc ON r.id = rc.restaurant_id
        JOIN categories c ON rc.category_id = c.id
        JOIN restaurant_subcategory rs ON r.id = rs.restaurant_id
        JOIN subcategories s ON rs.subcategory_id = s.id
        """

        where_clauses = []
        if categories != 'ALL':
            categories_q = ", ".join([f"'{i}'" for i in categories.split(';')])
            where_clauses.append(f"c.name IN ({categories_q})")
        if subcategories != 'ALL':
            subcategories_q = ", ".join([f"'{i}'" for i in subcategories.split(';')])
            where_clauses.append(f"s.name IN ({subcategories_q})")
        if places != 'ALL':
            places_q = ", ".join([f"'{i}'" for i in places.split(';')])
            where_clauses.append(f"r.nombre IN ({places_q})")

        if where_clauses:
            sql_query += " WHERE " + " AND ".join(where_clauses)

        sql_query += ";"

        cursor.execute(sql_query)
        result = cursor.fetchall()

        df = pd.DataFrame(result, columns=['name', 'category', 'subcategory', 'latitude', 'longitude', 'description', 'pais',
                                            'municipio', 'departamento', 'direccion', 'telefono', 'email', 'url', 'red_social', 
                                            'imagen', 'resena', 'hora_apertura', 'menu', 'destacado'])
        
        df[['latitude', 'longitude']] = df[['latitude', 'longitude']].astype(float)

        df['dict'] = df.apply(lambda x: {'display_name': x['name'], 'category': x['category'], 'subcategories': x['subcategory'], 
                                         'lat': x['latitude'], 'lon': x['longitude'], 'desc': x['description'], 'country':x['pais'], 
                                         'department':x['departamento'], 'municipality':x['municipio'], 'address':x['direccion'],
                                         'phone':x['telefono'], 'email':x['email'], 'url':x['url'], 'red_social':x['red_social'], 
                                         'image':x['imagen'], 'review':x['resena'], 'open_hour':x['hora_apertura'], 'menu':x['menu'], 'featured':x['destacado']}, axis=1)
        
        result_dict = list(df['dict'])

        response = {
        'data': result_dict,
        'ntotal': len(result_dict),
        'message': 'ok'
        }

        return Response(json.dumps(response, ensure_ascii=False), mimetype='application/json')

    except Exception as e:
        print(f"Error: {e}")
        response = {
        'message': f"error: {e}"
        }
        
        return Response(json.dumps(response, ensure_ascii=False), mimetype='application/json')


if __name__ == '__main__':
    app.config.from_object(config['development'])
    app.run()
