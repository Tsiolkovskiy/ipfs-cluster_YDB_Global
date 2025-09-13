package policy

// PolicyTemplates contains predefined policy templates for common use cases
var PolicyTemplates = map[string]*Policy{
	"basic-replication": {
		ID:   "basic-replication",
		Name: "Basic Replication Policy",
		Rules: map[string]string{
			"replication_factor": `
package replication_factor

# Default replication factor
default allow = 2

# Small files (< 1MB) get RF=2
allow = 2 {
	input.request.size < 1048576
}

# Medium files (1MB - 100MB) get RF=3
allow = 3 {
	input.request.size >= 1048576
	input.request.size < 104857600
}

# Large files (>= 100MB) get RF=4
allow = 4 {
	input.request.size >= 104857600
}`,
		},
		Metadata: map[string]string{
			"description": "Size-based replication factor policy",
			"category":    "replication",
			"version":     "1.0",
		},
	},

	"priority-based-placement": {
		ID:   "priority-based-placement",
		Name: "Priority-Based Placement Policy",
		Rules: map[string]string{
			"placement_constraints": `
package placement_constraints

# Default: single zone placement
default allow = {
	"min_zones": "1",
	"max_zones": "1"
}

# Critical priority (9-10): Multi-zone with specific regions
allow = constraints {
	input.request.priority >= 9
	constraints := {
		"min_zones": "3",
		"required_zones": "MSK,NN,SPB",
		"distribution": "even"
	}
}

# High priority (7-8): Multi-zone
allow = constraints {
	input.request.priority >= 7
	input.request.priority < 9
	constraints := {
		"min_zones": "2",
		"preferred_zones": "MSK,NN",
		"distribution": "even"
	}
}

# Medium priority (4-6): Flexible placement
allow = constraints {
	input.request.priority >= 4
	input.request.priority < 7
	constraints := {
		"min_zones": "1",
		"max_zones": "2",
		"cost_optimize": "true"
	}
}`,
		},
		Metadata: map[string]string{
			"description": "Placement policy based on content priority",
			"category":    "placement",
			"version":     "1.0",
		},
	},

	"erasure-coding-efficiency": {
		ID:   "erasure-coding-efficiency",
		Name: "Erasure Coding Efficiency Policy",
		Rules: map[string]string{
			"erasure_coding": `
package erasure_coding

# No erasure coding by default
default allow = null

# Large files (>= 100MB): Use EC for storage efficiency
allow = ec {
	input.request.size >= 104857600
	input.request.size < 1073741824  # < 1GB
	ec := {
		"data_shards": 4,
		"parity_shards": 2
	}
}

# Very large files (>= 1GB): More aggressive EC
allow = ec {
	input.request.size >= 1073741824
	ec := {
		"data_shards": 6,
		"parity_shards": 3
	}
}`,
		},
		Metadata: map[string]string{
			"description": "Erasure coding policy for storage efficiency",
			"category":    "erasure_coding",
			"version":     "1.0",
		},
	},

	"cost-optimization": {
		ID:   "cost-optimization",
		Name: "Cost Optimization Policy",
		Rules: map[string]string{
			"placement_constraints": `
package placement_constraints

# Default cost-aware placement
default allow = {
	"cost_optimize": "true",
	"prefer_local": "true"
}

# Archive content: Use cheapest storage
allow = constraints {
	input.request.constraints.archive
	constraints := {
		"storage_class": "cold",
		"min_zones": "1",
		"cost_optimize": "aggressive"
	}
}

# Frequently accessed content: Balance cost and performance
allow = constraints {
	input.request.constraints.access_pattern == "hot"
	constraints := {
		"storage_class": "hot",
		"min_zones": "2",
		"latency_optimize": "true"
	}
}`,
			"replication_factor": `
package replication_factor

# Cost-aware replication
default allow = 2

# Archive content: Minimal replication + EC
allow = 1 {
	input.request.constraints.archive
}

# Hot content: Higher replication for availability
allow = 3 {
	input.request.constraints.access_pattern == "hot"
}`,
		},
		Metadata: map[string]string{
			"description": "Cost optimization policy balancing storage costs and performance",
			"category":    "cost",
			"version":     "1.0",
		},
	},

	"compliance-policy": {
		ID:   "compliance-policy",
		Name: "Data Compliance Policy",
		Rules: map[string]string{
			"placement_constraints": `
package placement_constraints

# Default: No special compliance requirements
default allow = {}

# GDPR compliance: EU-only placement
allow = constraints {
	input.request.constraints.compliance == "gdpr"
	constraints := {
		"required_regions": "EU",
		"forbidden_regions": "US,APAC",
		"min_zones": "2"
	}
}

# Financial data: Strict placement and encryption
allow = constraints {
	input.request.constraints.data_classification == "financial"
	constraints := {
		"required_zones": "MSK,SPB",
		"encryption": "required",
		"audit_logging": "enabled",
		"min_zones": "2"
	}
}

# Personal data: Enhanced protection
allow = constraints {
	input.request.constraints.data_classification == "personal"
	constraints := {
		"encryption": "required",
		"access_logging": "enabled",
		"min_zones": "2",
		"retention_policy": "strict"
	}
}`,
		},
		Metadata: map[string]string{
			"description": "Data compliance and regulatory policy",
			"category":    "compliance",
			"version":     "1.0",
		},
	},

	"disaster-recovery": {
		ID:   "disaster-recovery",
		Name: "Disaster Recovery Policy",
		Rules: map[string]string{
			"placement_constraints": `
package placement_constraints

# Default DR strategy
default allow = {
	"min_zones": "1"
}

# Critical data: Multi-region DR
allow = constraints {
	input.request.priority >= 8
	constraints := {
		"min_zones": "3",
		"min_regions": "2",
		"cross_region_replication": "sync"
	}
}

# Important data: Multi-zone DR
allow = constraints {
	input.request.priority >= 5
	input.request.priority < 8
	constraints := {
		"min_zones": "2",
		"cross_zone_replication": "async",
		"backup_schedule": "daily"
	}
}`,
			"replication_factor": `
package replication_factor

# DR-aware replication factors
default allow = 2

# Critical data gets higher replication
allow = 4 {
	input.request.priority >= 8
}

# Important data gets standard replication
allow = 3 {
	input.request.priority >= 5
	input.request.priority < 8
}`,
		},
		Metadata: map[string]string{
			"description": "Disaster recovery and business continuity policy",
			"category":    "disaster_recovery",
			"version":     "1.0",
		},
	},
}

// GetPolicyTemplate returns a policy template by name
func GetPolicyTemplate(name string) (*Policy, bool) {
	template, exists := PolicyTemplates[name]
	if !exists {
		return nil, false
	}
	
	// Return a copy to avoid modification of the template
	copy := *template
	return &copy, true
}

// ListPolicyTemplates returns all available policy template names
func ListPolicyTemplates() []string {
	names := make([]string, 0, len(PolicyTemplates))
	for name := range PolicyTemplates {
		names = append(names, name)
	}
	return names
}

// PolicyCategory represents different categories of policies
type PolicyCategory string

const (
	CategoryReplication     PolicyCategory = "replication"
	CategoryPlacement       PolicyCategory = "placement"
	CategoryErasureCoding   PolicyCategory = "erasure_coding"
	CategoryCost           PolicyCategory = "cost"
	CategoryCompliance     PolicyCategory = "compliance"
	CategoryDisasterRecovery PolicyCategory = "disaster_recovery"
)

// GetPolicyTemplatesByCategory returns policy templates filtered by category
func GetPolicyTemplatesByCategory(category PolicyCategory) []*Policy {
	var policies []*Policy
	for _, template := range PolicyTemplates {
		if template.Metadata["category"] == string(category) {
			copy := *template
			policies = append(policies, &copy)
		}
	}
	return policies
}