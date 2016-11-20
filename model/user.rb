class User
  include Mongoid::Document
  include Mongoid::Timestamps
  # include Mongoid::Enum

  # has_many :authentications
  has_many :devices, inverse_of: :user,  autosave: true
  has_many :messages, class_name: 'Api::V1::Message', foreign_key: :new_uid

  ## Database authenticatable
  # enum :role, [:user, :admin]
  field :name
  field :email,              type: String, default: ""

  field :registration_id, type: String
  field :time_zone, type: Integer
  field :is_caregiver, type: Boolean, default: false

  field :authentication_token, type: String
  field :authentication_token_created_at, type: Time
  field :is_deleted, type: Boolean
  field :last_message_received_at, type: Integer, default: -1  #Epoch in milliseconds

  def valid_device

  end

  def generate_secure_token_string
    SecureRandom.urlsafe_base64(25).tr('lIO0', 'sxyz')
  end

  # Sarbanes-Oxley Compliance: http://en.wikipedia.org/wiki/Sarbanes%E2%80%93Oxley_Act
  def password_complexity
    if password.present? and not password.match(/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[\W]).+/)
      errors.add :password, "must include at least one of each: lowercase letter, uppercase letter, numeric digit, special character."
    end
  end

  def password_presence
    password.present? && password_confirmation.present?
  end

  def password_match
    password == password_confirmation
  end

  def ensure_authentication_token!
    # if authentication_token.blank? || authentication_token_expired
    if authentication_token.blank?
      self.authentication_token = generate_authentication_token
      self.authentication_token_created_at = Time.now
    end
  end

  def authentication_token_expired
    self.authentication_token_created_at.nil? || (self.authentication_token_created_at - Time.now) < -1209602
  end

  def generate_authentication_token
    loop do
      token = generate_secure_token_string
      break token unless User.where(authentication_token: token).first
    end
  end

  def reset_authentication_token!
    self.authentication_token = generate_authentication_token
    self.authentication_token_created_at = Time.now
  end

  def self.from_omniauth(auth)
    where(provider: auth.provider, uid: auth.uid).first_or_create do |user|
      user.provider = auth.provider
      user.uid = auth.uid
      user.email = auth.info.email
      user.password = Devise.friendly_token[0,20]
    end
  end

  def apply_omniauth(omniauth)
    authentications.build(:provider => omniauth['provider'], :uid => omniauth['uid'])
  end

  def password_required?
    (authentications.empty? || !password.blank?) && super
  end

  def existing_auth_providers
    ps = self.authentications.all

    if ps.size > 0
      return ps.map(&:provider)
    else
      return []
    end
  end
end
