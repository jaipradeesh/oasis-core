package identity

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oasislabs/oasis-core/go/common/crypto/signature"
	fileSigner "github.com/oasislabs/oasis-core/go/common/crypto/signature/signers/file"
)

func TestLoadOrGenerate(t *testing.T) {
	dataDir, err := ioutil.TempDir("", "oasis-identity-test_")
	require.NoError(t, err, "create data dir")
	defer os.RemoveAll(dataDir)

	factory := fileSigner.NewFactory(dataDir, signature.SignerNode, signature.SignerP2P, signature.SignerConsensus)

	// Generate a new identity.
	identity, err := LoadOrGenerate(dataDir, factory)
	require.NoError(t, err, "LoadOrGenerate")

	// Load an existing identity.
	identity2, err := LoadOrGenerate(dataDir, factory)
	require.NoError(t, err, "LoadOrGenerate (2)")
	require.EqualValues(t, identity.NodeSigner, identity2.NodeSigner)
	require.EqualValues(t, identity.P2PSigner, identity2.P2PSigner)
	require.EqualValues(t, identity.ConsensusSigner, identity2.ConsensusSigner)
	require.NotEqual(t, identity.TLSSigner, identity2.TLSSigner)
	require.NotEqual(t, identity.GetTLSCertificate(), identity2.GetTLSCertificate())
}
