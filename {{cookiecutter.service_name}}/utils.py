THEMATIC_SERVICES_KUBERNETES_MAPPING = {
    "forest": {
        "namespace": "user-forest",
        "service-account": "forest-sa",
    },
    "land": {
        "namespace": "user-land",
        "service-account": "land-sa",
    },
    "agriculture": {
        "namespace": "user-agriculture",
        "service-account": "agriculture-sa",
    },
    "security": {
        "namespace": "user-security",
        "service-account": "security-sa",
    },
    "water": {
        "namespace": "user-water",
        "service-account": "water-sa",
    },
    "internal": {
        "namespace": "user-internal",
        "service-account": "internal-sa",
    },
}

# Per-service Vault mapping
# Add/adjust secrets per service as needed.
# Minimal per-service config: ONLY role, name, path
THEMATIC_SERVICES_VAULT_MAPPING = {
    "forest": {
        "role": "forest-role",
        "name": "forest-secret",
        "path": "forest/secret"
    },
    "land": {
        "role": "land-role",
        "name": "land-secret",
        "path": "land/secret"
    },
    "agriculture": {
        "role": "agriculture-role",
        "name": "agriculture-secret",
        "path": "agriculture/secret",
    },
    "security": {
        "role": "security-role",
        "name": "security-secret",
        "path": "security/secret",
    },
    "water": {
        "role": "water-role",
        "name": "water-secret",
        "path": "water/secret"
    },
    "internal": {
        "role": "internal-role",
        "name": "internal-secret",
        "path": "internal/secret",
    },
}
