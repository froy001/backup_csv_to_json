module Api
  module V1
    class Message
      include Mongoid::Document
      include Mongoid::Timestamps

      belongs_to :user, class_name: "Api::V1::User", autosave: true, foreign_key: :uid
      belongs_to :new_user, class_name: "::User", foreign_key: :new_uid
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
      field :data_source, type: String

      # index({ created_at: 1}, { expire_after_seconds: 2.days.to_i })
      index({uid:1},{background: true})
      index({new_uid:1},{background: true})
      index({ts: 1},{background: true})
      validates_presence_of :mid
      validates_numericality_of :ts, only_integer: true
      validates_numericality_of :cts, only_integer: true

       # The user Model. Stores users in DB
      after_initialize :set_mid

      # A query scope returning only users yesterdays messages
      scope :yesterday, ->(user_id) { between(ts:(Utils::TimeUtils.yesterday_range user_id))}

      # Return messages by stream
      scope :heartRateModeSelected, -> {where(:"data.heartRateModeSelected".exists => true)}
      scope :skinTemperatureCollection, -> {where(:"data.skinTemperatureCollection".exists => true)}
      scope :skinTemperature, -> {where(:"data.skinTemperature".exists => true)}
      scope :gsrTonicCollection, -> {where(:"data.gsrTonicCollection".exists => true)}
      scope :gsrTonic, -> {where(:"data.gsrTonic".exists => true)}
      scope :hrvSelected, -> {where(:"data.hrvSelected".exists => true)}
      scope :hrvSDNN, -> {where(:"data.hrvSDNN".exists => true)}
      scope :activityAnnotation, -> {where(:"data.activityAnnotation".exists => true)}
      scope :stepsDaily, -> {where(:"data.stepsDaily".exists => true)}
      scope :accelMagCollection, -> {where(:"data.accelMagCollection".exists => true)}
      scope :userEventReport, -> {where(:"data.userEventReport".exists => true)}
      scope :"angelSensorsData.heartRate", -> {where(:"data.angelSensorsData.heartRate".exists => true)}
      scope :"angelSensorsData.stepCount", -> {where(:"data.angelSensorsData.stepCount".exists => true)}
      scope :"angelSensorsData.temperature", -> {where(:"data.angelSensorsData.temperature".exists => true)}
      scope :"angelSensorsData.accelerationEnergyMagnitude", -> {where(:"data.angelSensorsData.accelerationEnergyMagnitude".exists => true)}
      scope :deviceEvent, -> {where(:"data.deviceEvent".exists => true)}


      # Messages by abnormal event
      scope :fall_candidate_accelerometer, ->(user){
                                              if user.messages.yesterday(user.id).accelMagCollection
                                                where(
                                                  :"data.accelMagCollection".gte =>
                                                  (user.vitals_baseline.accel_mag_collection_avg * (1+user.vitals_baseline.accel_mag_collection_threshold))
                                                )
                                              else user.messages.yesterday(user.id).send("angelSensorsData.accelerationEnergyMagnitude")
                                                where(
                                                  :"angelSensorsData.accelerationEnergyMagnitude".gte =>
                                                  (user.vitals_baseline.accel_mag_collection_avg * (1+user.vitals_baseline.accel_mag_collection_threshold))
                                                )
                                              end
                                            }
    private
      def set_mid
        self.mid = self.id.to_s
        self.cts = 0
      end

    end
  end
end
