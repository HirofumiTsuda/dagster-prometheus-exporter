{{- define "dagster-prometheus-exporter.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dagster-prometheus-exporter.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "dagster-prometheus-exporter.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "dagster-prometheus-exporter.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "dagster-prometheus-exporter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dagster-prometheus-exporter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Where the CA bundle from dagsterGraphqlCA is mounted, and the file inside it
that DAGSTER_GRAPHQL_CA_FILE points at. Renders nothing when no source is set.
Also rejects a source given twice, which has no sensible meaning.
*/}}
{{- define "dagster-prometheus-exporter.graphqlCAFile" -}}
{{- $ca := .Values.dagsterGraphqlCA -}}
{{- if and $ca.existingSecret $ca.existingConfigMap -}}
{{- fail "set dagsterGraphqlCA.existingSecret or dagsterGraphqlCA.existingConfigMap, not both" -}}
{{- end -}}
{{- if or $ca.existingSecret $ca.existingConfigMap -}}
{{- printf "/etc/dagster-prometheus-exporter/ca/%s" $ca.key -}}
{{- end -}}
{{- end -}}
