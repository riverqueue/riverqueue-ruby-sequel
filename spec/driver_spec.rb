require "spec_helper"

class SimpleArgs
  attr_accessor :job_num

  def initialize(job_num:)
    self.job_num = job_num
  end

  def kind = "simple"

  def to_json = JSON.dump({job_num: job_num})
end

# Lets us test job-specific insertion opts by making `#insert_opts` an accessor.
# Real args that make use of this functionality will probably want to make
# `#insert_opts` a non-accessor method instead.
class SimpleArgsWithInsertOpts < SimpleArgs
  attr_accessor :insert_opts
end

RSpec.describe River::Driver::Sequel do
  around(:each) { |ex| test_transaction(&ex) }

  let!(:driver) { River::Driver::Sequel.new(DB) }
  let(:client) { River::Client.new(driver) }

  describe "#insert" do
    it "inserts a job" do
      job = client.insert(SimpleArgs.new(job_num: 1))
      expect(job).to have_attributes(
        attempt: 0,
        args: {"job_num" => 1},
        created_at: be_within(2).of(Time.now.utc),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        scheduled_at: be_within(2).of(Time.now.utc),
        state: River::JOB_STATE_AVAILABLE,
        tags: ::Sequel.pg_array([])
      )

      # Make sure it made it to the database. Assert only minimally since we're
      # certain it's the same as what we checked above.
      river_job = River::Driver::Sequel::RiverJob.first(id: job.id)
      expect(river_job).to have_attributes(
        kind: "simple"
      )
    end

    it "schedules a job" do
      target_time = Time.now.utc + 1 * 3600

      job = client.insert(
        SimpleArgs.new(job_num: 1),
        insert_opts: River::InsertOpts.new(scheduled_at: target_time)
      )
      expect(job).to have_attributes(
        scheduled_at: be_within(2).of(target_time),
        state: River::JOB_STATE_SCHEDULED
      )
    end

    it "inserts with job insert opts" do
      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(
        max_attempts: 23,
        priority: 2,
        queue: "job_custom_queue",
        tags: ["job_custom"]
      )

      job = client.insert(args)
      expect(job).to have_attributes(
        max_attempts: 23,
        priority: 2,
        queue: "job_custom_queue",
        tags: ["job_custom"]
      )
    end

    it "inserts with insert opts" do
      # We set job insert opts in this spec too so that we can verify that the
      # options passed at insertion time take precedence.
      args = SimpleArgsWithInsertOpts.new(job_num: 1)
      args.insert_opts = River::InsertOpts.new(
        max_attempts: 23,
        priority: 2,
        queue: "job_custom_queue",
        tags: ["job_custom"]
      )

      job = client.insert(args, insert_opts: River::InsertOpts.new(
        max_attempts: 17,
        priority: 3,
        queue: "my_queue",
        tags: ["custom"]
      ))
      expect(job).to have_attributes(
        max_attempts: 17,
        priority: 3,
        queue: "my_queue",
        tags: ["custom"]
      )
    end

    it "inserts with job args hash" do
      job = client.insert(River::JobArgsHash.new("hash_kind", {
        job_num: 1
      }))
      expect(job).to have_attributes(
        args: {"job_num" => 1},
        kind: "hash_kind"
      )
    end

    it "inserts in a transaction" do
      job = nil

      DB.transaction(savepoint: true) do
        job = client.insert(SimpleArgs.new(job_num: 1))

        river_job = River::Driver::Sequel::RiverJob.first(id: job.id)
        expect(river_job).to_not be_nil

        raise Sequel::Rollback
      end

      # Not visible because the job was rolled back.
      river_job = River::Driver::Sequel::RiverJob.first(id: job.id)
      expect(river_job).to be_nil
    end
  end

  describe "#to_job_row" do
    it "converts a database record to `River::JobRow`" do
      now = Time.now.utc
      river_job = River::Driver::Sequel::RiverJob.new(
        attempt: 1,
        attempted_at: now,
        attempted_by: ["client1"],
        created_at: now,
        args: %({"job_num":1}),
        finalized_at: now,
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: now,
        state: River::JOB_STATE_COMPLETED,
        tags: ["tag1"]
      )
      river_job.id = 1

      job_row = driver.send(:to_job_row, river_job)

      expect(job_row).to be_an_instance_of(River::JobRow)
      expect(job_row).to have_attributes(
        id: 1,
        args: {"job_num" => 1},
        attempt: 1,
        attempted_at: now,
        attempted_by: ["client1"],
        created_at: now,
        finalized_at: now,
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        scheduled_at: now,
        state: River::JOB_STATE_COMPLETED,
        tags: ["tag1"]
      )
    end

    it "with errors" do
      now = Time.now.utc
      river_job = River::Driver::Sequel::RiverJob.new(
        errors: [JSON.dump(
          {
            at: now,
            attempt: 1,
            error: "job failure",
            trace: "error trace"
          }
        )]
      )

      job_row = driver.send(:to_job_row, river_job)

      expect(job_row.errors.count).to be(1)
      expect(job_row.errors[0]).to be_an_instance_of(River::AttemptError)
      expect(job_row.errors[0]).to have_attributes(
        at: now.floor(0),
        attempt: 1,
        error: "job failure",
        trace: "error trace"
      )
    end
  end
end