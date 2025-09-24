-- Create user_assets table to track which assets each user wants
CREATE TABLE user_assets (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    asset VARCHAR(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create unique constraint to prevent duplicate user/asset combinations
CREATE UNIQUE INDEX idx_user_assets_unique ON user_assets(user_id, asset);

-- Create index for faster user lookups
CREATE INDEX idx_user_assets_user_id ON user_assets(user_id);

-- Insert sample asset preferences for Jeff Curry (assuming user_id = 1)
INSERT INTO user_assets (user_id, asset) VALUES 
    (1, 'USDC'),
    (1, 'BTC');