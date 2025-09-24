-- Create addresses table to store generated deposit addresses
CREATE TABLE addresses (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    asset VARCHAR(10) NOT NULL,
    network VARCHAR(50) NOT NULL,
    address VARCHAR(255) NOT NULL,
    wallet_id VARCHAR(255) NOT NULL,
    account_identifier VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Create index for user/asset lookups
CREATE INDEX idx_addresses_user_asset ON addresses(user_id, asset);

-- Create index for address lookups
CREATE INDEX idx_addresses_address ON addresses(address);

-- Create index for wallet_id lookups
CREATE INDEX idx_addresses_wallet_id ON addresses(wallet_id);

-- Create index for created_at for sorting
CREATE INDEX idx_addresses_created_at ON addresses(created_at);