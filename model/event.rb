class Event
  include Mongoid::Document
  include Mongoid::Timestamps

  belongs_to :user, dependent: :nullify
  has_one :message, class_name: "Api::V1::Message"

  validates_presence_of :user_id

  field :ts, type: Integer
end
