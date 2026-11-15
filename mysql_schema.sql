-- Optional source schema for a local or managed MySQL instance.
-- Apply this to a database you control; the pipeline only reads from it.
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id VARCHAR(64) NOT NULL PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    order_date DATE NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(12, 2) NOT NULL DEFAULT 0,
    promised_delivery_date DATE NULL,
    delivered_date DATE NULL,
    status VARCHAR(32) NOT NULL,
    INDEX idx_order_items_date (order_date),
    INDEX idx_order_items_customer (customer_id),
    INDEX idx_order_items_product (product_id)
);
