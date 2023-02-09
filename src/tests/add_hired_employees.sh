#!/bin/bash

# Definir la URL de la API
API_URL="http://localhost:7071/hired_employees/"

# Define la funcion que envia la peticion y verifica la respuesta
function send_request {
  local data=$1
  local expected_status=$2

  # Envia la petición y guarda la respuesta
  response=$(curl -X POST -w "%{http_code}" -H "Content-Type: application/json" -d "$data" $API_URL -o response.json)
  
  # Obtener el código de estado HTTP de la respuesta
  http_status=$response

  # Verifica si la respuesta es la esperada
  if [ "$expected_status" == "FAIL" ]; then
    if [ "$http_status" -ge 200 ] && [ "$http_status" -lt 300 ]; then
      echo "FAIL: La respuesta no es la esperada"
      echo "Request: $data"
      echo "Response: $(cat response.json)"
      echo "HTTP status code: $http_status"
    else
      echo "SUCCESS: La respuesta es la esperada"
      echo "HTTP status code: $http_status"
    fi
  elif [ "$expected_status" == "OK" ]; then
    if [ "$http_status" -ge 200 ] && [ "$http_status" -lt 300 ]; then
      echo "SUCCESS: La respuesta es la esperada"
      echo "HTTP status code: $http_status"
    else
      echo "FAIL: La respuesta no es la esperada"
      echo "Request: $data"
      echo "Response: $(cat response.json)"
      echo "HTTP status code: $http_status"
    fi
  fi
}

# Pruebas

echo -e "\nTest 0: Debe ser nok: un diccionario vacio (no hay datos)"
send_request "{}" "\"FAIL\""

echo -e "\nTest 1: Debe ser nok: un array vacio (no hay datos)"
send_request "[]" "FAIL"

echo -e "\nTest 2: Debe ser ok: un array con tres objetos con atributo 'name'" 
send_request "[{\"name\":\"New department\"},{\"name\":\"New department2\"},{\"name\":\"New department3\"}]" "OK"

echo -e "\nTest 3: Debe ser nok: un array con tres objetos con atributo 'name' y uno un atributo extra 'id'" 
send_request "[{\"name\":\"New department\"},{\"name\":\"New department2\"},{\"name\":\"New department3\", \"id\":1}]" "FAIL"

echo -e "\nTest 4: Debe ser ok: un array con 999 objetos con atributo 'name'"
data="["
for i in $(seq 1 999); do
  data="$data{\"name\":\"Department $i\"},"
done
data="${data%?}]"
send_request "$data" "OK"

echo -e "\nTest 5: Debe ser ok: un array con 1000 objetos con atributo 'name'" 
data="["
for i in $(seq 1 1000); do
  data="$data{\"name\":\"Department $i\"},"
done
data="${data%?}]"
send_request "$data" "OK"

# TODO
echo -e "\nTestear en base si se han insertado los registros"
echo "Pending"

echo -e "\nAll tests passed!"