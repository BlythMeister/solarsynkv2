#!/usr/bin/with-contenv bashio
set +e

# =============================================================================
# SolarSynk Home Assistant Integration Script - Refactored Version
# =============================================================================

# Configuration and Constants
readonly SCRIPT_DIR="$(dirname "$0")"
readonly PASSWORD_PUBLIC_KEY_FILE="password_public_key.pem"
readonly PASSWORD_PLAINTEXT_FILE="password_plaintext.txt"
readonly LOG_SEPARATOR="------------------------------------------------------------------------------"

# Initialize configuration variables
declare -A CONFIG
declare -A SENSOR_DATA
declare curlError=0
declare ServerAPIBearerToken=""
declare HTTP_Connect_Type="http"

# =============================================================================
# Utility Functions
# =============================================================================

log_message() {
    local level="$1"
    local message="${2:-}"
    local timestamp=$(date '+%d/%m/%Y %H:%M:%S')
    
    case "$level" in
        "INFO")  echo "[$timestamp] INFO: $message" ;;
        "ERROR") echo "[$timestamp] ERROR: $message" ;;
        "DEBUG") [[ "${CONFIG[Enable_Verbose_Log]:-false}" == "true" ]] && echo "[$timestamp] DEBUG: $message" ;;
        "SEPARATOR") echo "$LOG_SEPARATOR" ;;
    esac
}

cleanup_temp_files() {
    # Only log if this isn't initial startup cleanup
    if [[ -n "${CONFIG[Enable_Verbose_Log]:-}" ]]; then
        log_message "DEBUG" "Cleaning up temporary files"
    fi
    rm -f "$PASSWORD_PUBLIC_KEY_FILE" "$PASSWORD_PLAINTEXT_FILE"
    rm -f pvindata.json griddata.json loaddata.json batterydata.json 
    rm -f outputdata.json dcactemp.json inverterinfo.json settings.json token.json
    rm -f tmpcurllog.json
}

# =============================================================================
# Configuration Management
# =============================================================================

load_configuration() {
    log_message "INFO" "Loading configuration"
    
    CONFIG[sunsynk_user]="$(bashio::config 'sunsynk_user')"
    CONFIG[sunsynk_pass]="$(bashio::config 'sunsynk_pass')"
    CONFIG[sunsynk_serial]="$(bashio::config 'sunsynk_serial')"
    CONFIG[HA_LongLiveToken]="$(bashio::config 'HA_LongLiveToken')"
    CONFIG[Home_Assistant_IP]="$(bashio::config 'Home_Assistant_IP')"
    CONFIG[Home_Assistant_PORT]="$(bashio::config 'Home_Assistant_PORT')"
    CONFIG[Refresh_rate]="$(bashio::config 'Refresh_rate')"
    CONFIG[Enable_HTTPS]="$(bashio::config 'Enable_HTTPS')"
    CONFIG[Enable_Verbose_Log]="$(bashio::config 'Enable_Verbose_Log')"
    CONFIG[Settings_Helper_Entity]="$(bashio::config 'Settings_Helper_Entity')"
    
    # Set HTTP connection type
    if [[ "${CONFIG[Enable_HTTPS]}" == "true" ]]; then
        HTTP_Connect_Type="https"
    fi
    
    # Validate required configuration
    local required_configs=("sunsynk_user" "sunsynk_pass" "sunsynk_serial" "HA_LongLiveToken" "Home_Assistant_IP")
    for config in "${required_configs[@]}"; do
        if [[ -z "${CONFIG[$config]}" ]]; then
            log_message "ERROR" "Required configuration '$config' is missing"
            return 1
        fi
    done
    
    log_message "INFO" "Configuration loaded successfully"
    log_message "DEBUG" "HTTP Connect type: $HTTP_Connect_Type"
    log_message "DEBUG" "Sunsynk User: ${CONFIG[sunsynk_user]}"
    log_message "DEBUG" "Sunsynk Serial: ${CONFIG[sunsynk_serial]}"
    
    return 0
}

# =============================================================================
# Authentication Functions
# =============================================================================

encrypt_password() {
    log_message "INFO" "Encrypting password"
    
    # Fetch public key
    local public_key
    public_key=$(curl -s 'https://api.sunsynk.net/anonymous/publicKey?source=sunsynk' | jq -r '.data')
    
    if [[ -z "$public_key" || "$public_key" == "null" ]]; then
        log_message "ERROR" "Could not fetch public key from API"
        return 1
    fi
    
    log_message "DEBUG" "Encryption Key: $public_key"
    
    # Save public key to file
    {
        echo "-----BEGIN PUBLIC KEY-----"
        echo "$public_key"
        echo "-----END PUBLIC KEY-----"
    } > "$PASSWORD_PUBLIC_KEY_FILE"
    
    # Save plaintext password
    echo -n "${CONFIG[sunsynk_pass]}" > "$PASSWORD_PLAINTEXT_FILE"
    
    # Encrypt password
    local encrypted_password
    encrypted_password=$(openssl pkeyutl -encrypt -pubin -inkey "$PASSWORD_PUBLIC_KEY_FILE" -in "$PASSWORD_PLAINTEXT_FILE" | base64 -w 0)
    
    if [[ -z "$encrypted_password" ]]; then
        log_message "ERROR" "Password encryption failed"
        rm -f "$PASSWORD_PUBLIC_KEY_FILE" "$PASSWORD_PLAINTEXT_FILE"
        return 1
    fi
    
    log_message "DEBUG" "Password encrypted successfully"
    
    # Clean up temporary files
    rm -f "$PASSWORD_PUBLIC_KEY_FILE" "$PASSWORD_PLAINTEXT_FILE"
    
    CONFIG[sunsynk_pass_encrypted]="$encrypted_password"
    return 0
}

get_bearer_token() {
    log_message "INFO" "Getting bearer token"
    
    local new_url="https://api.sunsynk.net/oauth/token/new"
    local default_url="https://api.sunsynk.net/oauth/token"
    local combinations=(
        "enc-default,${CONFIG[sunsynk_pass_encrypted]},$default_url"
        "enc-new,${CONFIG[sunsynk_pass_encrypted]},$new_url"
        "plain-default,${CONFIG[sunsynk_pass]},$default_url"
        "plain-new,${CONFIG[sunsynk_pass]},$new_url"
    )
    
    local backoff_times=(1 1 2 3 5 8 13)
    local max_attempts=$(( ${#backoff_times[@]} + 1 )) # Total attempts = length of array + 1 (for the initial attempt)
    
    # Try each authentication combination
    for combo in "${combinations[@]}"; do
        local IFS=','
        read -r combo_id password_to_use url_to_use <<< "$combo"
        
        log_message "DEBUG" "Trying authentication method: $combo_id (Max attempts: $max_attempts)"
        local attempt=1
        
        while (( attempt <= max_attempts )); do
            
            # --- Array Indexing Logic ---
            local sleep_time=0
            local sleep_index=$((attempt - 2)) # Index 0 is for the sleep before attempt 2
            
            if (( attempt > 1 )); then
                # Check if the index is valid for the array
                if (( sleep_index >= 0 && sleep_index < ${#backoff_times[@]} )); then
                    sleep_time=${backoff_times[sleep_index]}
                fi
            fi
            
            if (( sleep_time > 0 )); then
                log_message "DEBUG" "Attempt $((attempt - 1)) failed. Waiting $sleep_time seconds before attempt $attempt..."
                sleep "$sleep_time"
            fi
            
            # Attempt to get token
            if curl -s -f -S -k -X POST -H "Content-Type: application/json" "$url_to_use" \
                -d "{\"client_id\": \"csp-web\",\"grant_type\": \"password\",\"password\": \"$password_to_use\",\"source\": \"sunsynk\",\"username\": \"${CONFIG[sunsynk_user]}\"}" \
                -o token.json; then
                
                log_message "DEBUG" "Token request successful for $combo_id (Attempt $attempt)"
                
                if [[ "${CONFIG[Enable_Verbose_Log]}" == "true" ]]; then
                    log_message "DEBUG" "Raw token data:"
                    cat token.json
                fi
                
                ServerAPIBearerToken=$(jq -r '.data.access_token' token.json)
                local token_success=$(jq -r '.success' token.json)
                
                if [[ "$token_success" == "true" && -n "$ServerAPIBearerToken" && "$ServerAPIBearerToken" != "null" ]]; then
                    log_message "INFO" "Valid token retrieved using $combo_id"
                    log_message "INFO" "Bearer Token length: ${#ServerAPIBearerToken}"
                    return 0
                else
                    local token_msg=$(jq -r '.msg' token.json)
                    log_message "ERROR" "Invalid token received: $token_msg (Attempt $attempt of $max_attempts)"
                    ((attempt++)) 
                fi
            else
                log_message "ERROR" "Token request failed with curl exit code $?. (Attempt $attempt of $max_attempts)"
                ((attempt++))
            fi
        done
    done
    
    log_message "ERROR" "Failed to get valid token with all methods"
    return 1
}

# =============================================================================
# Data Fetching Functions
# =============================================================================

fetch_api_data() {
    log_message "INFO" "Fetching data for serial: ${CONFIG[sunsynk_serial]}"
    
    local current_date=$(date +%Y-%m-%d)
    local serial="${CONFIG[sunsynk_serial]}"
    local auth_header="authorization: Bearer $ServerAPIBearerToken"
    local content_header="Content-Type: application/json"
    
    curlError=0
    
    # Define API endpoints and their output files
    local -A endpoints=(
        ["pvindata.json"]="https://api.sunsynk.net/api/v1/inverter/$serial/realtime/input"
        ["griddata.json"]="https://api.sunsynk.net/api/v1/inverter/grid/$serial/realtime?sn=$serial"
        ["loaddata.json"]="https://api.sunsynk.net/api/v1/inverter/load/$serial/realtime?sn=$serial"
        ["batterydata.json"]="https://api.sunsynk.net/api/v1/inverter/battery/$serial/realtime?sn=$serial&lan=en"
        ["outputdata.json"]="https://api.sunsynk.net/api/v1/inverter/$serial/realtime/output"
        ["dcactemp.json"]="https://api.sunsynk.net/api/v1/inverter/$serial/output/day?lan=en&date=$current_date&column=dc_temp,igbt_temp"
        ["inverterinfo.json"]="https://api.sunsynk.net/api/v1/inverter/$serial"
        ["settings.json"]="https://api.sunsynk.net/api/v1/common/setting/$serial/read"
    )
    
    # Fetch data from all endpoints
    for output_file in "${!endpoints[@]}"; do
        local url="${endpoints[$output_file]}"
        log_message "DEBUG" "Fetching $output_file from $url"
        
        if ! curl -s -f -S -k -X GET -H "$content_header" -H "$auth_header" "$url" -o "$output_file"; then
            log_message "ERROR" "Request failed for $output_file"
            curlError=1
        fi
    done
    
    if [[ $curlError -eq 0 ]]; then
        log_message "INFO" "Data fetched successfully"
        return 0
    else
        log_message "ERROR" "Some data requests failed"
        return 1
    fi
}

# =============================================================================
# Data Processing Functions
# =============================================================================

parse_json_data() {
    log_message "INFO" "Parsing JSON data"
    
    if [[ $curlError -ne 0 ]]; then
        log_message "ERROR" "Skipping data parsing due to curl errors"
        return 1
    fi
    
    # Parse inverter information
    SENSOR_DATA[inverterinfo_brand]=$(jq -r '.data.brand' inverterinfo.json)
    SENSOR_DATA[inverterinfo_status]=$(jq -r '.data.status' inverterinfo.json)
    SENSOR_DATA[inverterinfo_runstatus]=$(jq -r '.data.runStatus' inverterinfo.json)
    SENSOR_DATA[inverterinfo_ratepower]=$(jq -r '.data.ratePower' inverterinfo.json)
    SENSOR_DATA[inverterinfo_plantid]=$(jq -r '.data.plant.id' inverterinfo.json)
    SENSOR_DATA[inverterinfo_plantname]=$(jq -r '.data.plant.name' inverterinfo.json)
    SENSOR_DATA[inverterinfo_serial]=$(jq -r '.data.sn' inverterinfo.json)
    SENSOR_DATA[inverterinfo_updateat]=$(jq -r '.data.updateAt' inverterinfo.json)
    
    # Parse battery data
    SENSOR_DATA[battery_capacity]=$(jq -r '.data.capacity' batterydata.json)
    SENSOR_DATA[battery_chargevolt]=$(jq -r '.data.chargeVolt' batterydata.json)
    SENSOR_DATA[battery_current]=$(jq -r '.data.current' batterydata.json)
    SENSOR_DATA[battery_dischargevolt]=$(jq -r '.data.dischargeVolt' batterydata.json)
    SENSOR_DATA[battery_power]=$(jq -r '.data.power' batterydata.json)
    SENSOR_DATA[battery_soc]=$(jq -r '.data.soc' batterydata.json)
    SENSOR_DATA[battery_temperature]=$(jq -r '.data.temp' batterydata.json)
    SENSOR_DATA[battery_type]=$(jq -r '.data.type' batterydata.json)
    SENSOR_DATA[battery_voltage]=$(jq -r '.data.voltage' batterydata.json)
    
    # Parse battery 1 data
    SENSOR_DATA[battery1_voltage]=$(jq -r '.data.batteryVolt1' batterydata.json)
    SENSOR_DATA[battery1_current]=$(jq -r '.data.batteryCurrent1' batterydata.json)
    SENSOR_DATA[battery1_power]=$(jq -r '.data.batteryPower1' batterydata.json)
    SENSOR_DATA[battery1_soc]=$(jq -r '.data.batterySoc1' batterydata.json)
    SENSOR_DATA[battery1_temperature]=$(jq -r '.data.batteryTemp1' batterydata.json)
    SENSOR_DATA[battery1_status]=$(jq -r '.data.status' batterydata.json)
    
    # Parse battery 2 data
    SENSOR_DATA[battery2_voltage]=$(jq -r '.data.batteryVolt2' batterydata.json)
    SENSOR_DATA[battery2_current]=$(jq -r '.data.batteryCurrent2' batterydata.json)
    SENSOR_DATA[battery2_chargevolt]=$(jq -r '.data.chargeVolt2' batterydata.json)
    SENSOR_DATA[battery2_dischargevolt]=$(jq -r '.data.dischargeVolt2' batterydata.json)
    SENSOR_DATA[battery2_power]=$(jq -r '.data.batteryPower2' batterydata.json)
    SENSOR_DATA[battery2_soc]=$(jq -r '.data.batterySoc2' batterydata.json)
    SENSOR_DATA[battery2_temperature]=$(jq -r '.data.batteryTemp2' batterydata.json)
    SENSOR_DATA[battery2_status]=$(jq -r '.data.batteryStatus2' batterydata.json)
    
    # Parse daily energy data
    SENSOR_DATA[day_battery_charge]=$(jq -r '.data.etodayChg' batterydata.json)
    SENSOR_DATA[day_battery_discharge]=$(jq -r '.data.etodayDischg' batterydata.json)
    SENSOR_DATA[day_grid_export]=$(jq -r '.data.etodayTo' griddata.json)
    SENSOR_DATA[day_grid_import]=$(jq -r '.data.etodayFrom' griddata.json)
    SENSOR_DATA[day_load_energy]=$(jq -r '.data.dailyUsed' loaddata.json)
    SENSOR_DATA[day_pv_energy]=$(jq -r '.data.etoday' pvindata.json)
    
    # Parse BMS detailed monitoring data
    SENSOR_DATA[bms_soc]=$(jq -r '.data.bmsSoc' batterydata.json)
    SENSOR_DATA[bms_voltage]=$(jq -r '.data.bmsVolt' batterydata.json)
    SENSOR_DATA[bms_current]=$(jq -r '.data.bmsCurrent' batterydata.json)
    SENSOR_DATA[bms_temperature]=$(jq -r '.data.bmsTemp' batterydata.json)
    
    # Parse grid data
    SENSOR_DATA[grid_connected_status]=$(jq -r '.data.status' griddata.json)
    SENSOR_DATA[grid_frequency]=$(jq -r '.data.fac' griddata.json)
    SENSOR_DATA[grid_powerac]=$(jq -r '.data.pac' griddata.json)
    SENSOR_DATA[grid_powerreactive]=$(jq -r '.data.qac' griddata.json)
    SENSOR_DATA[grid_powerfactor]=$(jq -r '.data.pf' griddata.json)
    SENSOR_DATA[grid_power]=$(jq -r '.data.vip[0].power' griddata.json)
    SENSOR_DATA[grid_voltage]=$(jq -r '.data.vip[0].volt' griddata.json)
    SENSOR_DATA[grid_current]=$(jq -r '.data.vip[0].current' griddata.json)
    SENSOR_DATA[grid_power1]=$(jq -r '.data.vip[1].power' griddata.json)
    SENSOR_DATA[grid_voltage1]=$(jq -r '.data.vip[1].volt' griddata.json)
    SENSOR_DATA[grid_current1]=$(jq -r '.data.vip[1].current' griddata.json)
    SENSOR_DATA[grid_power2]=$(jq -r '.data.vip[2].power' griddata.json)
    SENSOR_DATA[grid_voltage2]=$(jq -r '.data.vip[2].volt' griddata.json)
    SENSOR_DATA[grid_current2]=$(jq -r '.data.vip[2].current' griddata.json)
    
    # Parse inverter output data
    SENSOR_DATA[inverter_frequency]=$(jq -r '.data.fac' outputdata.json)
    SENSOR_DATA[inverter_current]=$(jq -r '.data.vip[0].current' outputdata.json)
    SENSOR_DATA[inverter_power]=$(jq -r '.data.vip[0].power' outputdata.json)
    SENSOR_DATA[inverter_voltage]=$(jq -r '.data.vip[0].volt' outputdata.json)
    SENSOR_DATA[inverter_current1]=$(jq -r '.data.vip[1].current' outputdata.json)
    SENSOR_DATA[inverter_power1]=$(jq -r '.data.vip[1].power' outputdata.json)
    SENSOR_DATA[inverter_voltage1]=$(jq -r '.data.vip[1].volt' outputdata.json)
    SENSOR_DATA[inverter_current2]=$(jq -r '.data.vip[2].current' outputdata.json)
    SENSOR_DATA[inverter_power2]=$(jq -r '.data.vip[2].power' outputdata.json)
    SENSOR_DATA[inverter_voltage2]=$(jq -r '.data.vip[2].volt' outputdata.json)
    
    # Parse load data
    SENSOR_DATA[load_frequency]=$(jq -r '.data.loadFac' loaddata.json)
    SENSOR_DATA[load_voltage]=$(jq -r '.data.vip[0].volt' loaddata.json)
    SENSOR_DATA[load_voltage1]=$(jq -r '.data.vip[1].volt' loaddata.json)
    SENSOR_DATA[load_voltage2]=$(jq -r '.data.vip[2].volt' loaddata.json)
    SENSOR_DATA[load_current]=$(jq -r '.data.vip[0].current' loaddata.json)
    SENSOR_DATA[load_current1]=$(jq -r '.data.vip[1].current' loaddata.json)
    SENSOR_DATA[load_current2]=$(jq -r '.data.vip[2].current' loaddata.json)
    SENSOR_DATA[load_power]=$(jq -r '.data.vip[0].power' loaddata.json)
    SENSOR_DATA[load_power1]=$(jq -r '.data.vip[1].power' loaddata.json)
    SENSOR_DATA[load_power2]=$(jq -r '.data.vip[2].power' loaddata.json)
    SENSOR_DATA[load_upsPowerL1]=$(jq -r '.data.upsPowerL1' loaddata.json)
    SENSOR_DATA[load_upsPowerL2]=$(jq -r '.data.upsPowerL2' loaddata.json)
    SENSOR_DATA[load_upsPowerL3]=$(jq -r '.data.upsPowerL3' loaddata.json)
    SENSOR_DATA[load_upsPowerTotal]=$(jq -r '.data.upsPowerTotal' loaddata.json)
    SENSOR_DATA[load_totalpower]=$(jq -r '.data.totalPower' loaddata.json)
    
    # Parse additional output data
    SENSOR_DATA[output_pac]=$(jq -r '.data.pac' outputdata.json)
    SENSOR_DATA[output_pinv]=$(jq -r '.data.pInv' outputdata.json)
    
    # Parse PV data
    SENSOR_DATA[pv1_current]=$(jq -r '.data.pvIV[0].ipv' pvindata.json)
    SENSOR_DATA[pv1_power]=$(jq -r '.data.pvIV[0].ppv' pvindata.json)
    SENSOR_DATA[pv1_voltage]=$(jq -r '.data.pvIV[0].vpv' pvindata.json)
    SENSOR_DATA[pv2_current]=$(jq -r '.data.pvIV[1].ipv' pvindata.json)
    SENSOR_DATA[pv2_power]=$(jq -r '.data.pvIV[1].ppv' pvindata.json)
    SENSOR_DATA[pv2_voltage]=$(jq -r '.data.pvIV[1].vpv' pvindata.json)
    SENSOR_DATA[pv3_current]=$(jq -r '.data.pvIV[2].ipv' pvindata.json)
    SENSOR_DATA[pv3_power]=$(jq -r '.data.pvIV[2].ppv' pvindata.json)
    SENSOR_DATA[pv3_voltage]=$(jq -r '.data.pvIV[2].vpv' pvindata.json)
    SENSOR_DATA[pv4_current]=$(jq -r '.data.pvIV[3].ipv' pvindata.json)
    SENSOR_DATA[pv4_power]=$(jq -r '.data.pvIV[3].ppv' pvindata.json)
    SENSOR_DATA[pv4_voltage]=$(jq -r '.data.pvIV[3].vpv' pvindata.json)
    
    # Parse settings data
    SENSOR_DATA[prog1_time]=$(jq -r '.data.sellTime1' settings.json)
    SENSOR_DATA[prog2_time]=$(jq -r '.data.sellTime2' settings.json)
    SENSOR_DATA[prog3_time]=$(jq -r '.data.sellTime3' settings.json)
    SENSOR_DATA[prog4_time]=$(jq -r '.data.sellTime4' settings.json)
    SENSOR_DATA[prog5_time]=$(jq -r '.data.sellTime5' settings.json)
    SENSOR_DATA[prog6_time]=$(jq -r '.data.sellTime6' settings.json)
    SENSOR_DATA[prog1_charge]=$(jq -r '.data.time1on' settings.json)
    SENSOR_DATA[prog2_charge]=$(jq -r '.data.time2on' settings.json)
    SENSOR_DATA[prog3_charge]=$(jq -r '.data.time3on' settings.json)
    SENSOR_DATA[prog4_charge]=$(jq -r '.data.time4on' settings.json)
    SENSOR_DATA[prog5_charge]=$(jq -r '.data.time5on' settings.json)
    SENSOR_DATA[prog6_charge]=$(jq -r '.data.time6on' settings.json)
    SENSOR_DATA[prog1_capacity]=$(jq -r '.data.cap1' settings.json)
    SENSOR_DATA[prog2_capacity]=$(jq -r '.data.cap2' settings.json)
    SENSOR_DATA[prog3_capacity]=$(jq -r '.data.cap3' settings.json)
    SENSOR_DATA[prog4_capacity]=$(jq -r '.data.cap4' settings.json)
    SENSOR_DATA[prog5_capacity]=$(jq -r '.data.cap5' settings.json)
    SENSOR_DATA[prog6_capacity]=$(jq -r '.data.cap6' settings.json)
    SENSOR_DATA[battery_shutdown_cap]=$(jq -r '.data.batteryShutdownCap' settings.json)
    SENSOR_DATA[use_timer]=$(jq -r '.data.peakAndVallery' settings.json)
    SENSOR_DATA[priority_load]=$(jq -r '.data.energyMode' settings.json)
    
    # Parse temperature data
    SENSOR_DATA[dc_temp]=$(jq -r '.data.infos[0].records[-1].value' dcactemp.json)
    SENSOR_DATA[ac_temp]=$(jq -r '.data.infos[1].records[-1].value' dcactemp.json)
    
    # Set overall state
    SENSOR_DATA[overall_state]="${SENSOR_DATA[inverterinfo_runstatus]}"
    
    log_message "INFO" "JSON data parsed successfully"
    return 0
}

display_inverter_info() {
    log_message "SEPARATOR"
    log_message "INFO" "Inverter Information"
    log_message "INFO" "Brand: ${SENSOR_DATA[inverterinfo_brand]}"
    log_message "INFO" "Status: ${SENSOR_DATA[inverterinfo_runstatus]}"
    log_message "INFO" "Max Watts: ${SENSOR_DATA[inverterinfo_ratepower]}"
    log_message "INFO" "Plant ID: ${SENSOR_DATA[inverterinfo_plantid]}"
    log_message "INFO" "Plant Name: ${SENSOR_DATA[inverterinfo_plantname]}"
    log_message "INFO" "Inverter S/N: ${SENSOR_DATA[inverterinfo_serial]}"
    log_message "INFO" "Data Valid At: ${SENSOR_DATA[inverterinfo_updateat]}"
    log_message "SEPARATOR"
}

display_verbose_data() {
    if [[ "${CONFIG[Enable_Verbose_Log]}" != "true" ]]; then
        return
    fi
    
    log_message "DEBUG" "Raw data per file"
    log_message "SEPARATOR"
    
    for file in pvindata.json griddata.json loaddata.json batterydata.json outputdata.json dcactemp.json inverterinfo.json settings.json; do
        if [[ -f "$file" ]]; then
            echo "$file"
            cat "$file"
            log_message "SEPARATOR"
        fi
    done
    
    log_message "DEBUG" "Values to send. If ALL values are NULL then something went wrong:"
    
    # Display all sensor values
    for key in "${!SENSOR_DATA[@]}"; do
        echo "$key: ${SENSOR_DATA[$key]}"
    done
    
    log_message "SEPARATOR"
}

# =============================================================================
# Home Assistant Integration Functions
# =============================================================================

send_to_home_assistant() {
    local entity_suffix="$1"
    local value="$2"
    local attributes="$3"
    local friendly_name="$4"
    
    if [[ -z "$value" || "$value" == "null" ]]; then
        return 0
    fi
    
    local url="$HTTP_Connect_Type://${CONFIG[Home_Assistant_IP]}:${CONFIG[Home_Assistant_PORT]}/api/states/sensor.solarsynk_$entity_suffix"
    local payload="{\"attributes\": {$attributes, \"friendly_name\": \"$friendly_name\"}, \"state\": \"$value\"}"
    local log_output=""
    
    if [[ "${CONFIG[Enable_Verbose_Log]}" != "true" ]]; then
        log_output="-o /dev/null"
    fi
    
    curl -s -k -X POST \
        -H "Authorization: Bearer ${CONFIG[HA_LongLiveToken]}" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "$url" $log_output
}

# Define sensor configurations
declare -A SENSOR_CONFIGS=(
    # Battery sensors
    ["battery_capacity"]="\"unit_of_measurement\": \"Ah\"|Battery Capacity"
    ["battery_chargevolt"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery Charge Voltage"
    ["battery_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Battery Current"
    ["battery_dischargevolt"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery Discharge Voltage"
    ["battery_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Battery Power"
    ["battery_soc"]="\"device_class\": \"battery\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"%\"|Battery SOC"
    ["battery_temperature"]="\"device_class\": \"temperature\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"°C\"|Battery Temp"
    ["battery_type"]="\"unit_of_measurement\": \"\"|Battery Type"
    ["battery_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery Voltage"
    ["day_battery_charge"]="\"device_class\": \"energy\", \"state_class\":\"total_increasing\", \"unit_of_measurement\": \"kWh\"|Daily Battery Charge"
    ["day_battery_discharge"]="\"device_class\": \"energy\", \"state_class\":\"total_increasing\", \"unit_of_measurement\": \"kWh\"|Daily Battery Discharge"
    
    # Battery 1 sensors
    ["battery1_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery 1 Voltage"
    ["battery1_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Battery 1 Current"
    ["battery1_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Battery 1 Power"
    ["battery1_soc"]="\"device_class\": \"battery\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"%\"|Battery 1 SOC"
    ["battery1_temperature"]="\"device_class\": \"temperature\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"°C\"|Battery 1 Temp"
    ["battery1_status"]="\"unit_of_measurement\": \"\"|Battery 1 Status"
    
    # Battery 2 sensors
    ["battery2_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery 2 Voltage"
    ["battery2_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Battery 2 Current"
    ["battery2_chargevolt"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery 2 Charge Voltage"
    ["battery2_dischargevolt"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Battery 2 Discharge Voltage"
    ["battery2_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Battery 2 Power"
    ["battery2_soc"]="\"device_class\": \"battery\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"%\"|Battery 2 SOC"
    ["battery2_temperature"]="\"device_class\": \"temperature\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"°C\"|Battery 2 Temp"
    ["battery2_status"]="\"unit_of_measurement\": \"\"|Battery 2 Status"
    
    # Daily energy sensors
    ["day_grid_export"]="\"device_class\": \"energy\", \"state_class\":\"total_increasing\", \"unit_of_measurement\": \"kWh\"|Daily Grid Export"
    ["day_grid_import"]="\"device_class\": \"energy\", \"state_class\":\"total_increasing\", \"unit_of_measurement\": \"kWh\"|Daily Grid Import"
    ["day_load_energy"]="\"device_class\": \"energy\", \"state_class\":\"total_increasing\", \"unit_of_measurement\": \"kWh\"|Daily Load Energy"
    ["day_pv_energy"]="\"device_class\": \"energy\", \"state_class\":\"total_increasing\", \"unit_of_measurement\": \"kWh\"|Daily PV Energy"
    
    # BMS detailed monitoring sensors
    ["bms_soc"]="\"device_class\": \"battery\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"%\"|BMS State of Charge"
    ["bms_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|BMS Voltage"
    ["bms_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|BMS Current"
    ["bms_temperature"]="\"device_class\": \"temperature\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"°C\"|BMS Temperature"
    
    # Grid sensors
    ["grid_connected_status"]="\"unit_of_measurement\": \"\"|Grid Connection Status"
    ["grid_frequency"]="\"device_class\": \"frequency\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"Hz\"|Grid Freq"
    ["grid_power_ac"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Grid Power AC"
    ["grid_power_reactive"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Grid Reactive Power"
    ["grid_power_factor"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Grid Power Factor"
    ["grid_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Grid Power"
    ["grid_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Grid Voltage"
    ["grid_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Grid Current"
    ["grid_power1"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Grid Power L1"
    ["grid_voltage1"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Grid Voltage L1"
    ["grid_current1"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Grid Current L1"
    ["grid_power2"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Grid Power L2"
    ["grid_voltage2"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Grid Voltage L2"
    ["grid_current2"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Grid Current L2"
    
    # Inverter sensors
    ["inverter_frequency"]="\"device_class\": \"frequency\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"Hz\"|Inverter Freq"
    ["inverter_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Inverter Current"
    ["inverter_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Inverter Power"
    ["inverter_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Inverter Voltage"
    ["inverter_current1"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Inverter Current L1"
    ["inverter_power1"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Inverter Power L1"
    ["inverter_voltage1"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Inverter Voltage L1"
    ["inverter_current2"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Inverter Current L2"
    ["inverter_power2"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Inverter Power L2"
    ["inverter_voltage2"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Inverter Voltage L2"
    
    # Load sensors
    ["load_frequency"]="\"device_class\": \"frequency\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"Hz\"|Load Freq"
    ["load_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load Power"
    ["load_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Load Voltage"
    ["load_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Load Current"
    ["load_power1"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load Power L1"
    ["load_voltage1"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Load Voltage L1"
    ["load_current1"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Load Current L1"
    ["load_power2"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load Power L2"
    ["load_voltage2"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|Load Voltage L2"
    ["load_current2"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|Load Current L2"
    ["load_totalpower"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load Total Power"
    ["load_upsPowerL1"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load UPS Power L1"
    ["load_upsPowerL2"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load UPS Power L2"
    ["load_upsPowerL3"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load UPS Power L3"
    ["load_upsPowerTotal"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Load UPS Power Total"
    
    # Output/Inverter additional sensors
    ["output_pac"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Output AC Power"
    ["output_pinv"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|Inverter Input Power"
    
    # PV sensors
    ["pv1_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|PV1 Current"
    ["pv1_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|PV1 Power"
    ["pv1_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|PV1 Voltage"
    ["pv2_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|PV2 Current"
    ["pv2_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|PV2 Power"
    ["pv2_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|PV2 Voltage"
    ["pv3_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|PV3 Current"
    ["pv3_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|PV3 Power"
    ["pv3_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|PV3 Voltage"
    ["pv4_current"]="\"device_class\": \"current\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"A\"|PV4 Current"
    ["pv4_power"]="\"device_class\": \"power\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"W\"|PV4 Power"
    ["pv4_voltage"]="\"device_class\": \"voltage\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"V\"|PV4 Voltage"
    
    # Settings sensors - Program times
    ["prog1_time"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog1 Time"
    ["prog2_time"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog2 Time"
    ["prog3_time"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog3 Time"
    ["prog4_time"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog4 Time"
    ["prog5_time"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog5 Time"
    ["prog6_time"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog6 Time"
    ["prog1_charge"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog1 Charge"
    ["prog2_charge"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog2 Charge"
    ["prog3_charge"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog3 Charge"
    ["prog4_charge"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog4 Charge"
    ["prog5_charge"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog5 Charge"
    ["prog6_charge"]="\"device_class\": \"timestamp\", \"unit_of_measurement\": \"\"|Prog6 Charge"
    ["prog1_capacity"]="\"unit_of_measurement\": \"%\"|Prog1 Capacity"
    ["prog2_capacity"]="\"unit_of_measurement\": \"%\"|Prog2 Capacity"
    ["prog3_capacity"]="\"unit_of_measurement\": \"%\"|Prog3 Capacity"
    ["prog4_capacity"]="\"unit_of_measurement\": \"%\"|Prog4 Capacity"
    ["prog5_capacity"]="\"unit_of_measurement\": \"%\"|Prog5 Capacity"
    ["prog6_capacity"]="\"unit_of_measurement\": \"%\"|Prog6 Capacity"
    ["battery_shutdown_cap"]="\"device_class\": \"battery\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"%\"|Battery Shutdown Cap"
    ["use_timer"]="\"unit_of_measurement\": \"\"|Use Timer"
    ["priority_load"]="\"unit_of_measurement\": \"\"|Priority Load"
    
    # Temperature and other sensors
    ["inverterinfo_updateat"]="\"device_class\": \"timestamp\", \"state_class\":\"measurement\"|Updated At"
    ["overall_state"]="\"unit_of_measurement\": \"\"|Inverter Overall State"
    ["dc_temp"]="\"device_class\": \"temperature\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"°C\"|Inverter DC Temp"
    ["ac_temp"]="\"device_class\": \"temperature\", \"state_class\":\"measurement\", \"unit_of_measurement\": \"°C\"|Inverter AC Temp"
)

send_all_sensors_to_ha() {
    log_message "INFO" "Attempting to update sensor entities"
    log_message "INFO" "Sending to $HTTP_Connect_Type://${CONFIG[Home_Assistant_IP]}:${CONFIG[Home_Assistant_PORT]}"
    log_message "SEPARATOR"
    
    # Send all configured sensors
    for sensor_key in "${!SENSOR_CONFIGS[@]}"; do
        if [[ -n "${SENSOR_DATA[$sensor_key]}" ]]; then
            IFS='|' read -r attributes friendly_name <<< "${SENSOR_CONFIGS[$sensor_key]}"
            send_to_home_assistant "$sensor_key" "${SENSOR_DATA[$sensor_key]}" "$attributes" "$friendly_name"
        fi
    done
    
    log_message "INFO" "Sensor updates completed"
}

# =============================================================================
# Settings Management Functions
# =============================================================================

handle_inverter_settings() {
    log_message "SEPARATOR"
    log_message "INFO" "Reading settings entity -> solarsynk_inverter_settings"
    log_message "SEPARATOR"
    
    local check_entity
    check_entity=$(curl -s -k -X GET \
        -H "Authorization: Bearer ${CONFIG[HA_LongLiveToken]}" \
        -H "Content-Type: application/json" \
        "$HTTP_Connect_Type://${CONFIG[Home_Assistant_IP]}:${CONFIG[Home_Assistant_PORT]}/api/states/input_text.solarsynk_inverter_settings" | jq -r '.message')
    
    if [[ "$check_entity" == "Entity not found." ]]; then
        log_message "INFO" "Entity does not exist! Manually create it for this inverter using the HA GUI"
        log_message "INFO" "[Settings] -> [Devices & Services] -> [Helpers] tab -> [+ CREATE HELPER]"
        log_message "INFO" "Choose [Text] and name it [solarsynk_inverter_settings]"
        log_message "INFO" "Settings pushback system aborted. This is optional functionality."
        log_message "SEPARATOR"
    else
        local inverter_settings
        inverter_settings=$(curl -s -k -X GET \
            -H "Authorization: Bearer ${CONFIG[HA_LongLiveToken]}" \
            -H "Content-Type: application/json" \
            "$HTTP_Connect_Type://${CONFIG[Home_Assistant_IP]}:${CONFIG[Home_Assistant_PORT]}/api/states/input_text.solarsynk_inverter_settings" | jq -r '.state')
        
        if [[ -z "$inverter_settings" || "$inverter_settings" == "null" ]]; then
            log_message "INFO" "Helper entity has no value. No inverter settings will be changed."
        else
            log_message "INFO" "Updating inverter settings: $inverter_settings"
            curl -s -k -X POST \
                -H "Content-Type: application/json" \
                -H "authorization: Bearer $ServerAPIBearerToken" \
                "https://api.sunsynk.net/api/v1/common/setting/${CONFIG[sunsynk_serial]}/set" \
                -d "$inverter_settings" | jq -r '.'
        fi
        
        # Clear settings to prevent repeated application
        log_message "INFO" "Clearing temporary settings"
        curl -s -k -X POST \
            -H "Authorization: Bearer ${CONFIG[HA_LongLiveToken]}" \
            -H "Content-Type: application/json" \
            -d '{"attributes": {"unit_of_measurement": "", "friendly_name": "solarsynk_inverter_settings"}, "state": ""}' \
            "$HTTP_Connect_Type://${CONFIG[Home_Assistant_IP]}:${CONFIG[Home_Assistant_PORT]}/api/states/input_text.solarsynk_inverter_settings" > /dev/null
    fi
}

# =============================================================================
# Main Execution Loop
# =============================================================================

main_loop() {
    while true; do
        log_message "SEPARATOR"
        log_message "INFO" "SolarSynk - Log"
        log_message "SEPARATOR"
        local dt=$(date '+%d/%m/%Y %H:%M:%S')
        log_message "INFO" "Script execution date & time: $dt"
        
        cleanup_temp_files
        
        # Load configuration
        if ! load_configuration; then
            log_message "ERROR" "Configuration loading failed"
            log_message "INFO" "Script will retry in 300 seconds"
            sleep 300
            continue
        fi
        
        log_message "INFO" "Verbose logging is set to: ${CONFIG[Enable_Verbose_Log]}"
        
        # Encrypt password
        if ! encrypt_password; then
            log_message "ERROR" "Password encryption failed"
            log_message "INFO" "Script will retry in ${CONFIG[Refresh_rate]:-300} seconds"
            sleep "${CONFIG[Refresh_rate]:-300}"
            continue
        fi
        
        # Get bearer token
        if ! get_bearer_token; then
            log_message "ERROR" "Failed to get bearer token. Possible causes:"
            log_message "ERROR" "- Incorrect setup, check configuration"
            log_message "ERROR" "- Network connectivity issues"
            log_message "ERROR" "- Sunsynk server issues"
            log_message "ERROR" "- Too frequent connection requests"
            log_message "INFO" "Script will continue to loop but no values will be updated"
            sleep "${CONFIG[Refresh_rate]:-300}"
            continue
        fi
        
        log_message "INFO" "Sunsynk Server API Token: Hidden for security reasons"
        log_message "INFO" "Refresh rate set to: ${CONFIG[Refresh_rate]} seconds"
        log_message "INFO" "Note: Setting refresh rate lower than SunSynk server update rate won't increase actual update rate"
        
        # Fetch and process data
        if fetch_api_data && parse_json_data; then
            display_inverter_info
            display_verbose_data
            send_all_sensors_to_ha
            handle_inverter_settings
            log_message "INFO" "Fetch complete for inverter: ${CONFIG[sunsynk_serial]}"
        else
            log_message "ERROR" "Data processing failed"
        fi
        
        log_message "INFO" "All Done! Waiting ${CONFIG[Refresh_rate]} seconds to rinse and repeat"
        sleep "${CONFIG[Refresh_rate]}"
    done
}

# =============================================================================
# Script Entry Point
# =============================================================================

# Trap cleanup on script exit
trap cleanup_temp_files EXIT

# Start main execution loop
main_loop
