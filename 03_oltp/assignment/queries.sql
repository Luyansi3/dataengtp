-- Unpaid purchases (no payment recorded)
SELECT p.PurchaseID, p.PurchaseDateTime, c.FirstName, c.LastName, s.StoreName
FROM Purchases p
JOIN Customers c ON c.CustomerID = p.CustomerID
JOIN Stores s ON s.StoreID = p.StoreID
LEFT JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
WHERE pay.PurchaseID IS NULL
ORDER BY p.PurchaseDateTime DESC;

-- Customer’s last 5 purchases with item count and paid amount
SELECT SUM(pi.Quantity) AS Quantity, SUM(pay.Amount), p.PurchaseID, p.PurchaseDateTime
FROM Purchases p
JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
JOIN Purchase_items pi ON pi.PurchaseID = p.PurchaseID
WHERE p.CustomerID = (
	SELECT CustomerID FROM Customers WHERE FirstName = 'Marco' AND LastName = 'Dubois'
)
GROUP BY p.PurchaseID
ORDER BY p.PurchaseDateTime DESC
LIMIT 5;

-- Revenue & transaction count per store in a date range
SELECT s.StoreID, SUM(pay.Amount), COUNT(pay.Amount)
FROM Stores s
JOIN Purchases p ON p.StoreID = s.StoreID
JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
WHERE p.PurchaseDateTime < '2026-08-16 14:20:00' AND p.PurchaseDateTime > '2024-08-14 14:20:00'
GROUP BY s.StoreID

-- Top 5 products by revenue in the last N days
SELECT pr.ProductID, pr.ProductName, SUM(pr.Price*pi.Quantity) AS Amount
FROM Products pr
JOIN Purchase_items pi ON pi.ProductID = pr.ProductID
JOIN Purchases p ON pi.PurchaseID = p.PurchaseID
JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
WHERE p.PurchaseDateTime >= CURRENT_DATE - INTERVAL '100 days'
GROUP BY pr.ProductID, pr.ProductName
ORDER BY Amount DESC
LIMIT 5

-- Suppliers for a given product (comma-separated)
SELECT STRING_AGG(s.SupplierName, ', ') AS Suppliers
FROM Suppliers s
JOIN Product_Suppliers ps ON ps.SupplierID = s.SupplierID
JOIN Products p ON p.ProductID = ps.ProductID
WHERE p.ProductID = '5001'

-- Monthly revenue by category and by store's location a. can you add the year-over-year growth
SELECT c.CategoryName, s.Location, EXTRACT(MONTH FROM p.PurchaseDateTime) AS Monthly, SUM(pr.Price*pi.Quantity)
FROM Products pr
JOIN Categories c ON c.CategoryID = pr.CategoryID
JOIN Purchase_items pi ON pi.ProductID = pr.ProductID
JOIN Purchases p ON pi.PurchaseID = p.PurchaseID
JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
JOIN Stores s ON s.StoreID = p.StoreID
GROUP BY c.CategoryName, s.Location, Monthly

-- Monthly customer retention (“customers who bought in month N AND month N−1”.)
SELECT c.FirstName
FROM Customers c
JOIN Purchases p ON p.CustomerID = c.CustomerID
JOIN Payments pay ON pay.PurchaseID = p.PurchaseID
WHERE c.CustomerID IN (
	SELECT c2.CustomerID
	FROM Customers c2
	JOIN Purchases p2 ON p2.CustomerID = c2.CustomerID
	JOIN Payments pay2 ON pay2.PurchaseID = p2.PurchaseID
	WHERE EXTRACT(MONTH FROM p2.PurchaseDateTime) = EXTRACT(MONTH FROM p.PurchaseDateTime) - 1
)

-- Update a customer’s email (safely avoid unique-email conflicts)
UPDATE Customers
SET Email = 'ng@example.com'
WHERE CustomerID = 1001
  AND NOT EXISTS (SELECT 1 FROM Customers WHERE Email = 'ng@example.com')
RETURNING CustomerID, FirstName, LastName, Email;

-- Move a purchase to a different store
UPDATE Purchases
SET StoreID = 3002
WHERE PurchaseID = 7001
RETURNING *;

-- Change payment method and amount for a purchase (idempotent on PurchaseID)
UPDATE Payments
SET paymentmethod = 'Card', Amount = 10
WHERE PurchaseID = 7003
RETURNING *;

-- Reassign a product to a category (create category if it doesn’t exist)
WITH new_category AS (
	INSERT INTO Categories (CategoryID, CategoryName)
	VALUES (106, 'New')
	ON CONFLICT (CategoryID) DO NOTHING
	RETURNING CategoryID
)

UPDATE Products
SET CategoryID = (SELECT * FROM new_category UNION SELECT CategoryID FROM Categories WHERE CategoryID = '106')
WHERE ProductID = 5005
RETURNING *;


-- Optimistic price change (only if current price matches expected old price)
UPDATE Products
SET Price = 5.99
WHERE ProductID = 5002 AND Price = 4.00
RETURNING *;

