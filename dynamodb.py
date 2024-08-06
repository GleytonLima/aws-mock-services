from datetime import datetime
from typing import Dict, Any

from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import NotFound

app = Flask(__name__)

# Simulated DynamoDB data
DYNAMODB_TABLE: Dict[str, Dict[str, Dict[str, Any]]] = {
    'product_info': {
        '7db3a25a-0156-4aa3-8ef3-3d3b04fc59ca': {
            'cost': {'S': '10'},
            'msrp': {'S': '20'},
            'id': {'S': '7db3a25a-0156-4aa3-8ef3-3d3b04fc59ca'},
            'deleted': {'BOOL': False}
        },
        '3b3b4f3b-4f3b-4f3b-4f3b-4f3b4f3b4f3b': {
            'cost': {'S': '20'},
            'msrp': {'S': '30'},
            'id': {'S': '3b3b4f3b-4f3b-4f3b-4f3b-4f3b4f3b4f3b'},
            'deleted': {'BOOL': True}
        },
    }
}


@app.route('/<table_name>', methods=['GET'])
def get_table(table_name: str) -> tuple[Response, int]:
    """Get all items from a table."""
    if table_name not in DYNAMODB_TABLE:
        return jsonify({'error': 'Table not found'}), 404

    items = list(DYNAMODB_TABLE[table_name].values())
    return jsonify({
        'Items': items,
        'Count': len(items),
        'ScannedCount': len(items)
    }), 200


@app.route('/', methods=['POST'])
def dynamodb_operations() -> tuple[Response, int]:
    """Handle various DynamoDB operations."""
    print(request.get_json(force=True))
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({'error': 'Invalid request format'}), 400

        table_name = data.get('TableName')
        if not table_name:
            return jsonify({'error': 'TableName is required'}), 400

        if 'Item' in data:
            return handle_put_item(table_name, data['Item'])
        elif 'AttributeUpdates' in data:
            return handle_update_item(table_name, data)
        elif 'Key' in data and 'Expected' in data:
            return handle_delete_item(table_name, data)
        elif 'Key' in data:
            return handle_get_item(table_name, data)
        else:
            return handle_scan(table_name)

    except Exception as e:
        return jsonify({'error': str(e)}), 400


def handle_put_item(table_name: str, item: Dict[str, Any]) -> tuple[Response, int]:
    """Handle PutItem operation."""
    item_id = item.get('id', {}).get('S')
    if not item_id:
        print("Item ID is required")
        return jsonify({'error': 'Item ID is required'}), 400

    if table_name not in DYNAMODB_TABLE:
        print("Table not found")
        DYNAMODB_TABLE[table_name] = {}

    if item_id in DYNAMODB_TABLE[table_name]:
        print("Item already exists")
        return jsonify({'error': 'Item already exists'}), 400

    item['deleted'] = {'BOOL': False}
    DYNAMODB_TABLE[table_name][item_id] = item
    return jsonify({}), 200


def handle_update_item(table_name: str, data: Dict[str, Any]) -> tuple[Response, int]:
    """Handle UpdateItem operation."""
    item_id = data.get('Key', {}).get('id', {}).get('S')
    item_data = DYNAMODB_TABLE.get(table_name, {}).get(item_id)

    if not item_data:
        return jsonify({'error': 'Item not found'}), 404

    attribute_updates = data.get('AttributeUpdates', {})
    for attr, update_action in attribute_updates.items():
        action = update_action.get('Action')
        value = update_action.get('Value')
        if action == 'PUT':
            item_data[attr] = value

    return jsonify({'Attributes': item_data}), 200


def handle_delete_item(table_name: str, data: Dict[str, Any]) -> tuple[Response, int]:
    """Handle DeleteItem operation (Soft Delete)."""
    item_id = data.get('Key', {}).get('id', {}).get('S')
    if not item_id:
        return jsonify({'error': 'Item ID is required'}), 400

    if table_name not in DYNAMODB_TABLE or item_id not in DYNAMODB_TABLE[table_name]:
        return jsonify({'error': 'Item not found'}), 404

    DYNAMODB_TABLE[table_name][item_id]['deleted'] = {'BOOL': True}
    DYNAMODB_TABLE[table_name][item_id]['deletedAt'] = {'S': datetime.now().isoformat()}
    return jsonify({}), 200


def handle_get_item(table_name: str, data: Dict[str, Any]) -> tuple[Response, int]:
    """Handle GetItem operation."""
    item_id = data.get('Key', {}).get('id', {}).get('S')
    if not item_id:
        return jsonify({'error': 'Item ID is required'}), 400

    if table_name not in DYNAMODB_TABLE:
        return jsonify({
            '__type': 'ResourceNotFoundException',
            'message': 'Cannot do operations on a non-existent table'
        }), 400

    item = DYNAMODB_TABLE[table_name].get(item_id)
    return jsonify({'Item': item}) if item else jsonify({}), 200


def handle_scan(table_name: str) -> tuple[Response, int]:
    """Handle Scan operation (List items)."""
    if table_name not in DYNAMODB_TABLE:
        return jsonify({
            '__type': 'ResourceNotFoundException',
            'message': 'Cannot do operations on a non-existent table'
        }), 400

    items = list(DYNAMODB_TABLE[table_name].values())
    return jsonify({
        'Items': items,
        'Count': len(items),
        'ScannedCount': len(items)
    }), 200


@app.errorhandler(404)
def not_found(error: NotFound) -> tuple[Response, int]:
    """Handle 404 errors."""
    return jsonify({'error': 'Not found'}), 404


if __name__ == '__main__':
    app.run(port=5000, debug=True)
