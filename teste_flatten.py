import unittest

class TestFlattenFunctions(unittest.TestCase):
    def test_flatten_dict(self):
        input_data = {
            "user_info": {"user_id": 123, "name": "John Doe"},
            "address_info": {"city": "New York", "zipcode": "10001"},
            "order_details": {"product_id": 456, "quantity": 2}
        }
        expected_output = {
            'user_info_user_id': 123,
            'user_info_name': 'John Doe',
            'address_info_city': 'New York',
            'address_info_zipcode': '10001',
            'order_details_product_id': 456,
            'order_details_quantity': 2
        }
        self.assertEqual(flatten_dict(input_data), expected_output)

    def test_flatten_complex_data(self):
        input_data = {
            "data": [
                {"user_info": {"user_id": 123, "name": "John Doe"}, "order_details": {"product_id": 456, "quantity": 2}},
                {"user_info": {"user_id": 789, "name": "Alice Smith"}, "order_details": {"product_id": 789, "quantity": 3}}
            ],
            "metadata": {"timestamp": "2023-06-01", "source": "web"}
        }
        expected_output = {
            'data': [
                {'user_info_user_id': 123, 'user_info_name': 'John Doe', 'order_details_product_id': 456, 'order_details_quantity': 2},
                {'user_info_user_id': 789, 'user_info_name': 'Alice Smith', 'order_details_product_id': 789, 'order_details_quantity': 3}
            ],
            'metadata_timestamp': '2023-06-01',
            'metadata_source': 'web'
        }
        self.assertEqual(flatten_complex_data(input_data), expected_output)

if __name__ == '__main__':
    unittest.main()
