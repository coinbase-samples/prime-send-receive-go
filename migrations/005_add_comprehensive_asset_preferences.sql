-- Add comprehensive asset preferences for all users
-- Jeff Curry (user_id = 1) - already has USDC and BTC

-- Alice Johnson (user_id = 2) - wants both assets
INSERT INTO user_assets (user_id, asset) VALUES 
    (2, 'USDC'),
    (2, 'BTC');

-- Bob Smith (user_id = 3) - only wants BTC
INSERT INTO user_assets (user_id, asset) VALUES 
    (3, 'BTC');

-- Carol Williams (user_id = 4) - only wants USDC
INSERT INTO user_assets (user_id, asset) VALUES 
    (4, 'USDC');

-- David Brown (user_id = 5) - wants both assets
INSERT INTO user_assets (user_id, asset) VALUES 
    (5, 'USDC'),
    (5, 'BTC');

-- Eve Davis (user_id = 6) - only wants USDC
INSERT INTO user_assets (user_id, asset) VALUES 
    (6, 'USDC');

-- Frank Miller (user_id = 7) - wants both assets
INSERT INTO user_assets (user_id, asset) VALUES 
    (7, 'USDC'),
    (7, 'BTC');

-- Grace Wilson (user_id = 8) - only wants BTC
INSERT INTO user_assets (user_id, asset) VALUES 
    (8, 'BTC');

-- Henry Moore (user_id = 9) - wants both assets
INSERT INTO user_assets (user_id, asset) VALUES 
    (9, 'USDC'),
    (9, 'BTC');

-- Ivy Taylor (user_id = 10) - only wants USDC
INSERT INTO user_assets (user_id, asset) VALUES 
    (10, 'USDC');

-- Jack Anderson (user_id = 11) - wants both assets
INSERT INTO user_assets (user_id, asset) VALUES 
    (11, 'USDC'),
    (11, 'BTC');