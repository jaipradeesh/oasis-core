// Package identity encapsulates the node identity.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"path/filepath"
	"sync"

	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	"github.com/oasislabs/oasis-core/go/common/crypto/signature/signers/memory"
	tlsCert "github.com/oasislabs/oasis-core/go/common/crypto/tls"
)

const (
	// NodeKeyPubFilename is the filename of the PEM encoded node public key.
	NodeKeyPubFilename = "identity_pub.pem"

	// P2PKeyPubFilename is the filename of the PEM encoded p2p public key.
	P2PKeyPubFilename = "p2p_pub.pem"

	// ConsensusKeyPubFilename is the filename of the PEM encoded consensus
	// public key.
	ConsensusKeyPubFilename = "consensus_pub.pem"

	// CommonName is the CommonName to use when generating TLS certificates.
	CommonName = "oasis-node"
)

// Identity is a node identity.
type Identity struct {
	sync.RWMutex

	// NodeSigner is a node identity key signer.
	NodeSigner signature.Signer
	// P2PSigner is a node P2P link key signer.
	P2PSigner signature.Signer
	// ConsensusSigner is a node consensus key signer.
	ConsensusSigner signature.Signer
	// TLSSigner is a node TLS certificate signer.
	TLSSigner signature.Signer
	// TLSCertificate is a certificate that can be used for TLS.
	tlsCertificate *tls.Certificate
	// NextTLSCertificate is a certificate that can be used for TLS in the next epoch.
	nextTLSCertificate *tls.Certificate
}

// RotateCertificates rotates the TLS certificates.
// This is called on each epoch change.
func (i *Identity) RotateCertificates() error {
	i.Lock()
	defer i.Unlock()

	if i.tlsCertificate != nil {
		// Use the prepared certificate.
		if i.nextTLSCertificate != nil {
			i.tlsCertificate = i.nextTLSCertificate
		}

		// Generate a new TLS certificate to be used in the next epoch.
		var err error
		i.nextTLSCertificate, err = tlsCert.Generate(CommonName)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetTLSCertificate returns the current TLS certificate.
func (i *Identity) GetTLSCertificate() *tls.Certificate {
	i.RLock()
	defer i.RUnlock()

	return i.tlsCertificate
}

// SetTLSCertificate sets the current TLS certificate.
func (i *Identity) SetTLSCertificate(cert *tls.Certificate) {
	i.Lock()
	defer i.Unlock()

	i.tlsCertificate = cert
}

// GetNextTLSCertificate returns the next TLS certificate.
func (i *Identity) GetNextTLSCertificate() *tls.Certificate {
	i.RLock()
	defer i.RUnlock()

	return i.nextTLSCertificate
}

// SetNextTLSCertificate sets the next TLS certificate.
func (i *Identity) SetNextTLSCertificate(nextCert *tls.Certificate) {
	i.Lock()
	defer i.Unlock()

	i.nextTLSCertificate = nextCert
}

// Load loads an identity.
func Load(dataDir string, signerFactory signature.SignerFactory) (*Identity, error) {
	return doLoadOrGenerate(dataDir, signerFactory, false)
}

// LoadOrGenerate loads or generates an identity.
func LoadOrGenerate(dataDir string, signerFactory signature.SignerFactory) (*Identity, error) {
	return doLoadOrGenerate(dataDir, signerFactory, true)
}

func doLoadOrGenerate(dataDir string, signerFactory signature.SignerFactory, shouldGenerate bool) (*Identity, error) {
	var signers []signature.Signer
	for _, v := range []struct {
		role  signature.SignerRole
		pubFn string
	}{
		{signature.SignerNode, NodeKeyPubFilename},
		{signature.SignerP2P, P2PKeyPubFilename},
		{signature.SignerConsensus, ConsensusKeyPubFilename},
	} {
		signer, err := signerFactory.Load(v.role)
		switch err {
		case nil:
		case signature.ErrNotExist:
			if !shouldGenerate {
				return nil, err
			}
			if signer, err = signerFactory.Generate(v.role, rand.Reader); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}

		var checkPub signature.PublicKey
		if err = checkPub.LoadPEM(filepath.Join(dataDir, v.pubFn), signer); err != nil {
			return nil, err
		}

		signers = append(signers, signer)
	}

	// TLS certificate is always freshly generated.
	cert, err := tlsCert.Generate(CommonName)
	if err != nil {
		return nil, err
	}
	nextCert, err := tlsCert.Generate(CommonName)
	if err != nil {
		return nil, err
	}

	return &Identity{
		NodeSigner:         signers[0],
		P2PSigner:          signers[1],
		ConsensusSigner:    signers[2],
		TLSSigner:          memory.NewFromRuntime(cert.PrivateKey.(ed25519.PrivateKey)),
		tlsCertificate:     cert,
		nextTLSCertificate: nextCert,
	}, nil
}
