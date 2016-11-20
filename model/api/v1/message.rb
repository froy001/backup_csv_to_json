module Api
  module V1
    class Message
      include Mongoid::Document
      include Mongoid::Timestamps

      belongs_to :user, class_name: "Api::V1::User", foreign_key: :uid
      belongs_to :new_user, class_name: "::User", autosave: true, foreign_key: :new_uid
      belongs_to :event, class_name: "::Event", autosave: true
      belongs_to :gateway_device, class_name: "::GatewayDevice"

      # field :user_id, BSON::ObjectId
      field :uid, type: String
      field :new_uid, type: String
      field :mid, type: String
      field :data, type: Hash
      field :ts, type: Integer
      field :cts, type: Integer
      field :sdtid, type: String
      field :sdid, type: String
      field :sdid, type: String
      field :mv, type: Integer
    private
      def set_mid
        self.mid = self.id.to_s
      field :data_source, type: String

      # index({ created_at: 1}, { expire_after_seconds: 2.days.to_i })
      index({uid:1},{background: true})
      index({new_uid:1},{background: true})
      index({ts: 1},{background: true})
      # validates_presence_of :mid
      validates_numericality_of :ts, only_integer: true
      # validates_numericality_of :cts, only_integer: true

        self.cts = 0
      end

    end
  end
end
