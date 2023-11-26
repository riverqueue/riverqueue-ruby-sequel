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

  let(:client) { River::Client.new(River::Driver::Sequel.new(DB)) }

  describe "#insert" do
    it "inserts a job" do
      job = client.insert(SimpleArgs.new(job_num: 1))
      expect(job).to have_attributes(
        attempt: 0,
        created_at: be_within(2).of(Time.now),
        encoded_args: %({"job_num": 1}),
        kind: "simple",
        max_attempts: River::MAX_ATTEMPTS_DEFAULT,
        queue: River::QUEUE_DEFAULT,
        priority: River::PRIORITY_DEFAULT,
        scheduled_at: be_within(2).of(Time.now),
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
      target_time = Time.now + 1 * 3600

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
        encoded_args: %({"job_num": 1}),
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
end
