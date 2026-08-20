import axios from 'axios'
import { store } from '../state.js'

function base() {
  return store.apiBase
}

export async function listModels() {
  const { data } = await axios.get(`${base()}/modeling/models`)
  // model_server returns a JSON array of model summaries.
  return Array.isArray(data) ? data : (data.models ?? [])
}

export async function getModel(imageTag) {
  const { data } = await axios.get(
    `${base()}/modeling/models/${encodeURIComponent(imageTag)}`,
  )
  return data
}

export async function getModelJsonSchema(imageTag) {
  const { data } = await axios.get(
    `${base()}/modeling/models/${encodeURIComponent(imageTag)}/jsonschema`,
  )
  return data
}

export async function predict(image, input) {
  const { data } = await axios.post(`${base()}/modeling/predict`, { image, input })
  return data
}

export async function registerModel(payload) {
  const { data } = await axios.post(`${base()}/modeling/models`, payload)
  return data
}
