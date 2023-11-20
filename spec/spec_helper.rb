require "riverqueue"
require "riverqueue-sequel"
require "sequel"

DB = Sequel.connect(ENV["TEST_DATABASE_URL"] || "postgres://localhost/riverqueue_ruby_test")

def test_transaction
  DB.transaction do
    yield
    raise Sequel::Rollback
  end
end
